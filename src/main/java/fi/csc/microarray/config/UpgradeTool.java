package fi.csc.microarray.config;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.InetAddress;
import java.util.LinkedList;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import fi.csc.microarray.util.IOUtils;
import fi.csc.microarray.util.XmlUtil;

/**
 * Simple tool for upgrading existing installation to new version.
 * 
 * @author Aleksi Kallio
 *
 */
public class UpgradeTool {

	private LinkedList<Operation> operations = new LinkedList<Operation>();
	
	private static abstract class Operation {

		private File file;
		private Object parameter;

		public Operation(File file, Object parameter) {
			this.file = file;
			this.parameter = parameter;
		}
		
		public abstract void execute(File file, Object parameter);
		public abstract String describeExecution(File file, Object parameter);
	}
	
	
	public void upgrade(File pathToOld) throws Exception {
		
		// create the transaction
		createOperations(pathToOld);
		
		// tell what we are about to do
		for (Operation operation : operations) {
			System.out.println(operation.describeExecution(operation.file, operation.parameter));
		}
		
		// ask if we should commit
		System.out.println("");
		ConfigTool.verifyChanges(new BufferedReader(new InputStreamReader(System.in)));
		
		// do it
		for (Operation operation : operations) {
			try {
				operation.execute(operation.file, operation.parameter);
				
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("\nError occurred, continuing anyway...");
			}
		}
		
		// we are done
		System.out.println("\nAll changes successfully written!");
		System.out.println("\nYou need to regenerate server passwords (run genpasswd.sh)");
		
		
	}
	
	public void createOperations(File pathToOld) {

		// transform old installation components to new directory layout
		for (String componentDir : ConfigTool.getComponentDirsWithConfig()) {
			if (new File(pathToOld, componentDir).exists()) {

				// initialise paths
				File componentDir12x = new File(pathToOld, componentDir);
				File componentDir13x = new File(componentDir);
				if (!componentDir13x.exists() ) {
					throw new RuntimeException(componentDir13x.getAbsolutePath() + " not found, must be run inside complete 1.3.x installation");
				}
				File workDir12x = new File(componentDir12x, "nami-work-files");
				File logsDir13x = new File(componentDir13x, DirectoryLayout.LOGS_DIR);
				File confDir13x = new File(componentDir13x, DirectoryLayout.CONF_DIR);
				File securityDir13x = new File(componentDir13x, DirectoryLayout.SECURITY_DIR);

				// transform directory layout
				copyToNewDir(workDir12x, logsDir13x, "messages.log", "jobs.log", "security.log", "status.log");
				delayedCopy(new File(workDir12x, "nami.log"), new File(logsDir13x, "chipster.log"));
				copyToNewDir(workDir12x, securityDir13x, "keystore.ks", "users");
				delayedUsersUpgrade(new File(securityDir13x, "users"), new File(workDir12x, "users"));
				delayedCopy(new File(workDir12x, "nami-config.xml"), new File(confDir13x, "chipster-config.xml"));
				delayedConfigPurge(new File(confDir13x, "chipster-config.xml"), new File(workDir12x, "nami-config.xml"), componentDir);

				if (componentDir.equals("webstart")) {
					delayedCopy(new File(componentDir12x, "web-content" + File.separator + "chipster.jnlp"), new File(componentDir13x, "web-root" + File.separator + "chipster.jnlp"));
					delayedJnlpUpgrade(new File(componentDir13x, "web-root" + File.separator + "chipster.jnlp"), new File(componentDir12x, "web-content" + File.separator + "chipster.jnlp"));
					delayedCopy(new File(workDir12x, "nami-config.xml"), new File(componentDir13x, "web-root" + File.separator + "chipster-config.xml"));
					delayedConfigPurge(new File(componentDir13x, "web-root" + File.separator + "chipster-config.xml"), new File(workDir12x, "nami-config.xml"), "client");
				}

			}
		}		
	}

	private void copyToNewDir(File oldDir, File newDir, String... files) {
		
		// move listed files to new
		for (String file : files) {
			File from = new File(oldDir, file);
			File to = new File(newDir, file);
			delayedCopy(from, to);
		}
	}
	
	private void delayedCopy(File from, File to) {

		if (from.exists()) {
			operations.add(new Operation(from, to) {

				public String describeExecution(File file, Object parameter) {
					return "Copy " + file + " to " + parameter;
				}

				public void execute(File file, Object parameter) {
					try {
						IOUtils.copy(file, (File)parameter);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}		
				}
			});
		}
	}

	private void delayedUsersUpgrade(File fileAfterUpgrade, File filebeforeUpgrade) {
		if (filebeforeUpgrade.exists()) {
			operations.add(new Operation(fileAfterUpgrade, null) {

				public String describeExecution(File file, Object parameter) {
					return "Updgrade file format in " + file + " (add empty expiration dates if needed)";
				}

				public void execute(File file, Object parameter) {
					BufferedReader in = null;
					OutputStream overwriteOut = null;
					try {
						StringWriter stringWriter = new StringWriter();
						BufferedWriter out = new BufferedWriter(stringWriter);
						in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
						
						boolean needOverwrite = false;
						
						for (String line = in.readLine(); line != null; line = in.readLine()) {
							if (line.split(":").length == 3) {
								// has exactly two separators, need to add empty for exp. date
								int commentSep = line.lastIndexOf(':');
								String newLine = line.substring(0, commentSep) + ":" + line.substring(commentSep);
								line = newLine;
								needOverwrite = true;
							}
							out.write(line + "\n");
						}
						out.close();
						if (needOverwrite) {
							overwriteOut = new FileOutputStream(file);
							IOUtils.copy(new ByteArrayInputStream(stringWriter.getBuffer().toString().getBytes()), overwriteOut);
						}
						
					} catch (IOException e) {
						e.printStackTrace();
						System.out.println("Warning: could not upgrade users file " + file.getAbsolutePath());
						
					} finally {
						IOUtils.closeIfPossible(in);
						IOUtils.closeIfPossible(overwriteOut);
					}
				}			
			});
		}		
	}

	private void delayedConfigPurge(File fileAfterUpgrade, File filebeforeUpgrade, String componentDir) {
		if (filebeforeUpgrade.exists()) {
			operations.add(new Operation(fileAfterUpgrade, componentDir) {

				public String describeExecution(File file, Object parameter) {
					return "Upgrade and purge " + file + " (update entry names and leave only those modules needed for " + parameter + ")";
				}

				public void execute(File file, Object parameter) {
					BufferedReader in = null;
					OutputStream overwriteOut = null;
					try {
						StringWriter stringWriter = new StringWriter();
						BufferedWriter out = new BufferedWriter(stringWriter);
						in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
						
						Document originalDocument = XmlUtil.getInstance().parseReader(in);
						IOUtils.closeIfPossible(in);
						Document newDocument = XmlUtil.getInstance().newDocument();
						newDocument.appendChild(newDocument.createElement("configuration"));
						newDocument.getDocumentElement().setAttribute("content-version", "3");

						// pick correct modules
						moveModule("messaging", originalDocument, "messaging", newDocument);
						moveModule("security", originalDocument, "security", newDocument);
						if ("comp".equals(parameter)) {
							moveModule("analyser", originalDocument, "comp", newDocument);
							
						} else if ("frontend".equals(parameter)) {
							moveModule("frontend", originalDocument, "filebroker", newDocument);
							moveModule("filebroker", originalDocument, "filebroker", newDocument);
							
						} else if ("fileserver".equals(parameter)) {
							Element module = (Element)newDocument.getDocumentElement().appendChild(newDocument.createElement("configuration-module"));
							module.setAttribute("moduleId", "filebroker");
							addEntry(module, newDocument, "port", "8080");
							addEntry(module, newDocument, "url", "http://" + InetAddress.getLocalHost().getHostName());
							changeEntry(newDocument, "username", "filebroker");
							
						} else if ("webstart".equals(parameter)) {
							Element module = (Element)newDocument.getDocumentElement().appendChild(newDocument.createElement("configuration-module"));
							module.setAttribute("moduleId", "webstart");
							addEntry(module, newDocument, "port", "8081");
							
						} else if ("manager".equals(parameter)) {
							Element module = (Element)newDocument.getDocumentElement().appendChild(newDocument.createElement("configuration-module"));
							module.setAttribute("moduleId", "manager");
							addEntry(module, newDocument, "port", "8082");

						} else {
							moveModuleIfExists((String)parameter, originalDocument, (String)parameter, newDocument);
						}
						
						// remove obsolete entries
						NodeList entries = newDocument.getElementsByTagName("entry");
						for (int i = 0; i < entries.getLength(); i++) {
							Element entry = (Element)entries.item(i);
							String oldName = entry.getAttribute("entryKey");
							
							if ("filebroker_urls".equals(oldName)) {
								entry.getParentNode().removeChild(entry);
								break;
							}
						}
						
						// fix entry names
						entries = newDocument.getElementsByTagName("entry");
						for (int i = 0; i < entries.getLength(); i++) {
							Element entry = (Element)entries.item(i);
							String oldName = entry.getAttribute("entryKey");

							// rename
							oldName = oldName.replace('_', '-');
							String newName = "";
							for (int c = 0; c < oldName.length(); c++) {
								char character = oldName.charAt(c);
								if (Character.isUpperCase(character)) {
									newName += '-';
									newName += Character.toLowerCase(character);
								} else {
									newName += character;
								}
							}
							
							if (newName.startsWith("-")) {
								newName = newName.substring(1);
							}
							
							entry.setAttribute("entryKey", newName);
							
						}
												
						// overwrite previous
						XmlUtil.getInstance().printXml(newDocument, out);
						out.close();

						overwriteOut = new FileOutputStream(file);
						IOUtils.copy(new ByteArrayInputStream(stringWriter.getBuffer().toString().getBytes()), overwriteOut);
						
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println("Warning: could not upgrade configuration file " + file.getAbsolutePath());
						
					} finally {						
						IOUtils.closeIfPossible(overwriteOut);
					}
				}

				private void changeEntry(Document document, String name, String value) {
					NodeList entries = document.getDocumentElement().getElementsByTagName("entry");
					for (int i = 0; i < entries.getLength(); i++) {
						Element entry = (Element)entries.item(i);
						if (name.equals(entry.getAttribute("entryKey"))) {
							Element valueElement = (Element)entry.getElementsByTagName("value").item(0);
							valueElement.setTextContent(value);
							return;
						}
					}
					throw new IllegalArgumentException("entry " + name + " not found");
				}
				
				private void addEntry(Element module, Document document, String name, String value) {
					Element entry = (Element)module.appendChild(document.createElement("entry"));
					entry.setAttribute("entryKey", name);
					Element valueElement = (Element)entry.appendChild(document.createElement("value"));
					valueElement.setTextContent(value);
				}

				private void moveModuleIfExists(String originalModule, Document originalDocument, String newModule, Document newDocument) {
					try {
						moveModule(originalModule, originalDocument, newModule, newDocument);
					} catch (IllegalArgumentException e) {
						// module did not exist, ignore the exception 
					}
				}
				private void moveModule(String originalModule, Document originalDocument, String newModule, Document newDocument) {
					NodeList modules = originalDocument.getElementsByTagName("configuration-module");
					for (int i = 0; i < modules.getLength(); i++) {
						Element module = (Element)modules.item(i);
						if (originalModule.equals(module.getAttribute("moduleId"))) {
							Element importedElement = (Element)newDocument.importNode(module, true);
							newDocument.getDocumentElement().appendChild(importedElement);
							importedElement.setAttribute("moduleId", newModule);
							return;
						}
					}
					throw new IllegalArgumentException("module " + originalModule + " was not found from config");
				}			
			});
		}		
	}
	
	private void delayedJnlpUpgrade(File fileAfterUpgrade, File filebeforeUpgrade) {
		if (filebeforeUpgrade.exists()) {
			operations.add(new Operation(fileAfterUpgrade, null) {

				public String describeExecution(File file, Object parameter) {
					return "Upgrade " + file + " (change configuration passing mechanism to 1.3.x URL based)";
				}

				public void execute(File file, Object parameter) {
					BufferedReader in = null;
					OutputStream overwriteOut = null;
					try {
						StringWriter stringWriter = new StringWriter();
						BufferedWriter out = new BufferedWriter(stringWriter);
						in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
						
						Document document = XmlUtil.getInstance().parseReader(in);
						IOUtils.closeIfPossible(in);
										
						Element applicationDesc = (Element)document.getElementsByTagName("application-desc").item(0);
						while (applicationDesc.getElementsByTagName("argument").getLength() > 0) {
							applicationDesc.removeChild(applicationDesc.getElementsByTagName("argument").item(0));
							
						}
						addArgument(document, applicationDesc, "client");
						addArgument(document, applicationDesc, "-config");
						addArgument(document, applicationDesc, document.getDocumentElement().getAttribute("codebase") + "/chipster-config.xml");						
						
						// overwrite previous
						XmlUtil.getInstance().printXml(document, out);
						out.close();

						overwriteOut = new FileOutputStream(file);
						IOUtils.copy(new ByteArrayInputStream(stringWriter.getBuffer().toString().getBytes()), overwriteOut);
						
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println("Warning: could not upgrade jnlp file " + file.getAbsolutePath());
						
					} finally {						
						IOUtils.closeIfPossible(overwriteOut);
					}
				}

				private void addArgument(Document document, Element applicationDesc, String value) {
					Node node = applicationDesc.appendChild(document.createElement("argument"));
					node.setTextContent(value);					
				}

			});
		}		
	}
}



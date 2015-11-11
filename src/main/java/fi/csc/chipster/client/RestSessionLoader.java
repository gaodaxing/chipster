package fi.csc.chipster.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import fi.csc.chipster.auth.AuthenticationClient;
import fi.csc.chipster.rest.Config;
import fi.csc.chipster.rest.RestUtils;
import fi.csc.chipster.servicelocator.ServiceLocatorClient;
import fi.csc.chipster.sessiondb.model.Dataset;
import fi.csc.chipster.sessiondb.model.Input;
import fi.csc.chipster.sessiondb.model.Job;
import fi.csc.microarray.client.ClientApplication;
import fi.csc.microarray.client.NameID;
import fi.csc.microarray.client.Session;
import fi.csc.microarray.client.operation.OperationDefinition;
import fi.csc.microarray.client.operation.OperationRecord;
import fi.csc.microarray.client.operation.OperationRecord.ParameterRecord;
import fi.csc.microarray.client.operation.parameter.Parameter;
import fi.csc.microarray.client.session.SessionManager;
import fi.csc.microarray.client.tasks.TaskExecutor;
import fi.csc.microarray.databeans.DataBean;
import fi.csc.microarray.databeans.DataBean.Link;
import fi.csc.microarray.databeans.DataFolder;
import fi.csc.microarray.databeans.DataItem;
import fi.csc.microarray.databeans.DataManager;
import fi.csc.microarray.exception.MicroarrayException;
import fi.csc.microarray.filebroker.FileBrokerClient;

public class RestSessionLoader {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(RestSessionLoader.class);
	
	DataManager 		dataManager 	= Session.getSession().getDataManager();
	SessionManager 		sessionManager 	= Session.getSession().getApplication().getSessionManager();
	TaskExecutor 		taskExecutor 	= Session.getSession().getApplication().getTaskExecutor();
	FileBrokerClient 	fileBroker 		= Session.getSession().getServiceAccessor().getFileBrokerClient();
	
	ServiceLocatorClient serviceLocatorClient = new ServiceLocatorClient(new Config());
	WebTarget sessionDbTarget = new AuthenticationClient(serviceLocatorClient, "client", "clientPassword").getAuthenticatedClient().target("http://localhost:8001/sessiondb/");

	private LinkedHashMap<String, DataBean> dataBeans = new LinkedHashMap<String, DataBean>();
	private LinkedHashMap<String, OperationRecord> operationRecords = new LinkedHashMap<String, OperationRecord>();

	private Integer xOffset;
	
	private String sessionId;

	private String sessionNotes;

	public RestSessionLoader(String sessionId) {
		this.sessionId = sessionId;
		sessionManager.setSessionId(UUID.fromString(sessionId));
	}

	private void createDataBeans(fi.csc.chipster.sessiondb.model.Session session) {
		
		for (Dataset dataset : session.getDatasets().values()) {
			String name = dataset.getName();
			String id = dataset.getDatasetId().toString();
						
			// check for unique session id
			if (getDataItem(id) != null) {
				logger.warn("duplicate data bean id: " + id + " , ignoring data bean: " + name);
				continue;
			}
			
			// check that data id exists
			if (id == null) {
				logger.warn("could not load data bean: " + name + " due to missing data id");
				throw new RuntimeException("trying to load data without data id");
			} 
									
			DataBean dataBean;
			try {
				
				/* Don't ask content length from filebroker at this point,
				 * but do it later in parallel along the type tags.
				 */				
				dataBean = dataManager.createDataBean(name, id, false);		

				if (dataset.getFile() != null) {
					// Set file size from metadata. If there are external
					// ContentLocations, the size must match.
					dataManager.setOrVerifyContentLength(dataBean, dataset.getFile().getSize());
					// set checksum from the metadata, but the checksum of the real file is calculated only 
					// later during possible network transfers
					dataManager.setOrVerifyChecksum(dataBean, dataset.getFile().getChecksum());
				}
				
				Integer x = dataset.getY();
				Integer y = dataset.getX();
				
				if (x != null && y != null) {
					if (xOffset != null) {
						x += xOffset;
					}
					dataBean.setPosition(x, y);
				}
			
			} catch (Exception e) {
				Session.getSession().getApplication().reportExceptionThreadSafely(new Exception("error while opening file " + name, e));
				logger.warn("could not create data bean: " + name);
				continue;
			}
			
			
			// creation time
			UUID jobId = dataset.getSourceJob();
			Job job = session.getJobs().get(jobId);
			if (job != null) {
				dataBean.setCreationDate(RestUtils.toDate(job.getEndTime()));
			}

			dataBean.setNotes(dataset.getNotes());
			dataBean.setContentType(dataManager.guessContentType(dataBean.getName()));
			
			dataBeans.put(id, dataBean);
		}		
	}

	
	private void createOperations(fi.csc.chipster.sessiondb.model.Session session) {
		for (Job job : session.getJobs().values()) {
			String jobId = job.getJobId().toString();

			// check for unique id
			if (operationRecords.containsKey(jobId)) {
				logger.warn("duplicate operation id: " + jobId);
				continue;
			}

			OperationRecord operationRecord = new OperationRecord();

			// name id
			operationRecord.setNameID(new NameID(job.getToolId(), job.getToolName(), job.getToolDescription()));
			
			// category
			operationRecord.setCategoryName(job.getToolCategory());
			
//			String colorString = operationType.getCategoryColor();
//			if (colorString != null) {
//				operationRecord.setCategoryColor(Color.decode(colorString));
//			}

			// module
			operationRecord.setModule(job.getModule());
			
			// parameters
			for (fi.csc.chipster.sessiondb.model.Parameter param : job.getParameters()) {
				operationRecord.addParameter(param.getParameterId(), param.getDisplayName(), param.getDescription(), param.getValue());
			}

			// source code
			operationRecord.setSourceCode(job.getSourceCode());

			// update names, category from the current version of the tool
			ClientApplication application = Session.getSession().getApplication();
			OperationDefinition currentTool = null;
			
			if (application != null) { //there is no client application when filebroker handles example sessions				
				currentTool = application.getOperationDefinitionBestMatch(operationRecord.getNameID().getID(), operationRecord.getModule(), operationRecord.getCategoryName());
			}
				
			if (currentTool != null) {
				operationRecord.getNameID().setDisplayName(currentTool.getDisplayName());
				operationRecord.getNameID().setDescription(currentTool.getDescription());
				if (currentTool.getCategory().getModule() != null) {
					operationRecord.setModule(currentTool.getCategory().getModule().getModuleName());
				}
				operationRecord.setCategoryName(currentTool.getCategoryName());
				operationRecord.setCategoryColor(currentTool.getCategory().getColor());

				for (ParameterRecord parameterRecord : operationRecord.getParameters()) {
					Parameter currentParameter = currentTool.getParameter(parameterRecord.getNameID().getID());
					if (currentParameter != null) {
						parameterRecord.getNameID().setDisplayName(currentParameter.getDisplayName());
						parameterRecord.getNameID().setDescription(currentParameter.getDescription());
					}
				}
			}

			// job id for continuation
			operationRecord.setJobId(jobId);

			operationRecord.setStartTime(RestUtils.toDate(job.getStartTime()));
			operationRecord.setEndTime(RestUtils.toDate(job.getEndTime()));										
			
			// store the operation record
			operationRecords.put(jobId, operationRecord);
		}
	}

	
	private void linkDataItemChildren(DataFolder parent) {
		
		ArrayList<DataItem> children = new ArrayList<>();
		ArrayList<DataFolder> folders = new ArrayList<>();
		
		for (DataBean bean : dataBeans.values()) {
					
			// add as a child
			children.add(bean);
		}
		
		// connect children in parallel
		dataManager.connectChildren(children, dataManager.getRootFolder());
		
		for (DataFolder folder : folders) {
			linkDataItemChildren(folder);
		}
	}
	
	/**
	 * Link OperationRecords and DataBeans by adding real input DataBean references
	 * to OperationRecords.
	 * @param session 
	 * 
	 */
	private void linkInputsToOperations(fi.csc.chipster.sessiondb.model.Session session) {
		
		for (Job job : session.getJobs().values()) {
			
			// get data bean ids from session data
			for (Input input : job.getInputs()) {

				String inputID = input.getDatasetId();
				OperationRecord record = operationRecords.get(job.getJobId().toString());
				
				// data bean exists
				
				if (record != null) {
					DataBean inputBean = dataBeans.get(inputID);
					
					// add the reference to the operation record
					record.addInput(new NameID(input.getInputId(), input.getDisplayName(), input.getDescription()), inputBean);
				}
				
				// data bean does not exist
				else {
					
					System.out.println("operation record not found for job " + job.getToolName());
					
					
//					// add the reference to the operation record
//					if (inputID != null) {
//						operationRecords.get(job.getJobId()).addInput(new NameID(input.getInputId(), input.getDisplayName(), input.getDescription()), inputID);
//					}
				}
			}
		}
	}

	
	/**
	 * Add links form DataBeans to the OperationRecord which created the DataBean.
	 * 
	 * If OperationRecord is not found, use unknown OperationRecord.
	 * @param session 
	 * 
	 */
	private void linkOperationsToOutputs(fi.csc.chipster.sessiondb.model.Session session) {
		
		for (DataBean dataBean : dataBeans.values()) {
			
			Dataset dataset = session.getDatasets().get(UUID.fromString(dataBean.getId()));
			UUID jobId = dataset.getSourceJob();
			
			OperationRecord operationRecord = null;
			if (jobId != null) {
				operationRecord = operationRecords.get(jobId.toString());
				
				Job job = session.getJobs().get(jobId);
				for (Input input : job.getInputs()) {					
					DataBean target = dataBeans.get(input.getDatasetId());
					dataBean.addLink(Link.DERIVATION, target);
				}
			}

			// if operation record is not found use dummy
			if (operationRecord == null) {
				operationRecord = OperationRecord.getUnkownOperationRecord();
			}
			
			dataBean.setOperationRecord(operationRecord);
		}
	}

//	/**
//	 * @param session 
//	 * 
//	 */
//	private void linkDataBeans(fi.csc.chipster.sessiondb.model.Session session) {
//		for (DataBean dataBean : dataBeans.values()) {
//			for (LinkType linkType : dataTypes.get(dataBean).getLink()) {
//				// if something goes wrong for this link, continue with others
//				try {
//					String targetID = linkType.getTarget();
//					if (targetID != null) {
//						DataBean targetBean = dataBeans.get(targetID);
//						if (targetBean != null) {
//							dataBean.addLink(Link.valueOf(linkType.getType()), targetBean);
//						}
//					}
//					
//				} catch (Exception e) {
//					logger.warn("could not add link", e);
//					continue;
//				}
//			}
//		}
//	}
	
	/**
	 * 
	 * @param id
	 * @return null if no DataItem for the id is found
	 */
	private DataItem getDataItem(String id) {		 
		return dataBeans.get(id);
	}

	public void loadSession() throws Exception {
		
		fi.csc.chipster.sessiondb.model.Session session = getSession(UUID.fromString(sessionId)); 
		
		this.sessionNotes = session.getNotes();	
		
		createDataBeans(session);
		createOperations(session);
		linkOperationsToOutputs(session);
				
		linkDataItemChildren(dataManager.getRootFolder());
		//linkDataBeans(session);
		linkInputsToOperations(session);
		
		//getUnfinishedOperations(session);
	}
	
	private fi.csc.chipster.sessiondb.model.Session getSession(UUID sessionId) throws MicroarrayException {
		WebTarget sessionTarget = sessionDbTarget.path("sessions/" + sessionId.toString());
		Response response = sessionTarget.request().get(Response.class);
		if (!RestUtils.isSuccessful(response.getStatus())) {
			throw new MicroarrayException("get session failed: " + response.getStatus() + " " + response.readEntity(String.class) + " " + sessionTarget.getUri());
		}
		
		fi.csc.chipster.sessiondb.model.Session session = (fi.csc.chipster.sessiondb.model.Session) response.readEntity(fi.csc.chipster.sessiondb.model.Session.class);
		
		List<Dataset> datasets = getDatasets(sessionId);
		List<Job> jobs = getJobs(sessionId);
		
		HashMap<UUID, Dataset> datasetMap = new HashMap<>();
		HashMap<UUID, Job> jobMap = new HashMap<>();
		
		for (Dataset dataset : datasets) {
			datasetMap.put(dataset.getDatasetId(), dataset);
		}
		
		for (Job job : jobs) {
			jobMap.put(job.getJobId(), job);
		}
		
		session.setDatasets(datasetMap);
		session.setJobs(jobMap);
		
        return session;
	}
	
	private List<Dataset> getDatasets(UUID sessionId) throws MicroarrayException {
		WebTarget sessionTarget = sessionDbTarget.path("sessions/" + sessionId.toString() + "/datasets");
		Response response = sessionTarget.request().get(Response.class);
		if (!RestUtils.isSuccessful(response.getStatus())) {
			throw new MicroarrayException("get datasets failed: " + response.getStatus() + " " + response.readEntity(String.class) + " " + sessionTarget.getUri());
		}
		
		String json = response.readEntity(String.class);
		@SuppressWarnings("unchecked")
		List<Dataset> datasets = RestUtils.parseJson(List.class, Dataset.class, json);
        return datasets;
	}
	
	private List<Job> getJobs(UUID sessionId) throws MicroarrayException {
		WebTarget sessionTarget = sessionDbTarget.path("sessions/" + sessionId.toString() + "/jobs");
		Response response = sessionTarget.request().get(Response.class);
		if (!RestUtils.isSuccessful(response.getStatus())) {
			throw new MicroarrayException("get jobs failed: " + response.getStatus() + " " + response.readEntity(String.class) + " " + sessionTarget.getUri());
		}
		
		String json = response.readEntity(String.class);
		@SuppressWarnings("unchecked")
		List<Job> jobs = RestUtils.parseJson(List.class, Job.class, json);
        return jobs;
	}
	
	public void setXOffset(Integer xOffset) {
		this.xOffset = xOffset;
	}

	public List<OperationRecord> getUnfinishedOperations() {
		
		ArrayList<OperationRecord> unfinished = new ArrayList<>();
		
//		for (OperationRecord operationRecord : this.operationRecords.values()) {
//			String jobId = operationRecord.getJobId();
//			if (jobId != null) {
//				unfinished.add(operationRecord);
//			}
//		}
		return unfinished;
	}

	public String getSessionNotes() {
		return sessionNotes;
	}
}
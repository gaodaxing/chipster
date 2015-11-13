package fi.csc.chipster.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import javax.jms.JMSException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.glassfish.jersey.client.ClientProperties;

import fi.csc.chipster.rest.RestUtils;
import fi.csc.chipster.sessiondb.RestException;
import fi.csc.chipster.sessiondb.SessionDbClient;
import fi.csc.microarray.client.RemoteServiceAccessor;
import fi.csc.microarray.client.Session;
import fi.csc.microarray.filebroker.ChecksumException;
import fi.csc.microarray.filebroker.ChecksumInputStream;
import fi.csc.microarray.filebroker.DbSession;
import fi.csc.microarray.filebroker.FileBrokerClient;
import fi.csc.microarray.filebroker.FileBrokerException;
import fi.csc.microarray.messaging.admin.StorageAdminAPI.StorageEntryMessageListener;
import fi.csc.microarray.util.Exceptions;
import fi.csc.microarray.util.IOUtils;
import fi.csc.microarray.util.IOUtils.CopyProgressListener;

public class RestFileBrokerClient implements FileBrokerClient {
	
	@SuppressWarnings("unused")
	private static final Logger logger = Logger.getLogger(RestFileBrokerClient.class);
		
	private SessionDbClient sessionDbClient = ((RemoteServiceAccessor) Session.getSession().getServiceAccessor()).getSessionDbClient();
	private WebTarget fileBrokerTarget = ((RemoteServiceAccessor) Session.getSession().getServiceAccessor()).getRestFileBrokerClient();

	/**
	 * Add file to file broker. Must be a cached file, for other types, use other versions of this method.
	 * 
	 * @see fi.csc.microarray.filebroker.FileBrokerClient#addFile(File, CopyProgressListener)
	 */
	@Override
	public void addFile(String dataId, FileBrokerArea area, File file, CopyProgressListener progressListener) throws JMSException, IOException {
		
		InputStream inputStream = new FileInputStream(file);
		upload(getSessionId(), dataId, inputStream);
	}
	
	private void upload(UUID sessionId, String dataId, InputStream inputStream) throws IOException {
		String datasetPath = "sessions/" + getSessionId().toString() + "/datasets/" + dataId;
		WebTarget datasetTarget = fileBrokerTarget.path(datasetPath);

		// Use chunked encoding to disable buffering. HttpUrlConnector in 
		// Jersey buffers the whole file before sending it by default, which 
		// won't work with big files.
		fileBrokerTarget.property(ClientProperties .REQUEST_ENTITY_PROCESSING, "CHUNKED");
		Response response = datasetTarget.request().put(Entity.entity(inputStream, MediaType.APPLICATION_OCTET_STREAM), Response.class);
		if (!RestUtils.isSuccessful(response.getStatus())) {
			throw new IOException("upload failed: " + response.getStatus() + " " + response.readEntity(String.class) + " " + datasetTarget.getUri());
		}
	}

	private UUID getSessionId() {
		return Session.getSession().getApplication().getSessionManager().getSessionId();
	}

	/**
	 * @return md5 String of the uploaded data, if enabled in configuration
	 * @see fi.csc.microarray.filebroker.FileBrokerClient#addFile(InputStream, CopyProgressListener)
	 */
	@Override
	public String addFile(String dataId, FileBrokerArea area, InputStream file, long contentLength, CopyProgressListener progressListener) throws FileBrokerException, JMSException, IOException {
		
		upload(getSessionId(), dataId, file);
		return null;
	}
	
	@Override
	public ChecksumInputStream getInputStream(String dataId) throws IOException, JMSException {
	
		InputStream remoteStream = download(getSessionId(), dataId);
		return new ChecksumInputStream(remoteStream, true);					
	}

	
	private InputStream download(UUID sessionId, String dataId) throws JMSException {
		String datasetPath = "sessions/" + getSessionId().toString() + "/datasets/" + dataId;
		WebTarget datasetTarget = fileBrokerTarget.path(datasetPath);
		Response response = datasetTarget.request().get(Response.class);
		if (!RestUtils.isSuccessful(response.getStatus())) {
			// the interface shouldn't require JMSExceptions
			throw new JMSException("get input stream failed: " + response.getStatus() + " " + response.readEntity(String.class) + " " + datasetTarget.getUri());
		}
		return response.readEntity(InputStream.class);
	}

	/**
	 * Get a local copy of a file. If the dataId  matches any of the files found from 
	 * local filebroker paths (given in constructor of this class), then it is symlinked or copied locally.
	 * Otherwise the file pointed by the dataId is downloaded.
	 * @throws JMSException 
	 * @throws ChecksumException 
	 * 
	 * @see fi.csc.microarray.filebroker.FileBrokerClient#getFile(File, URL)
	 */
	@Override
	public void getFile(String dataId, File destFile) throws IOException, JMSException, ChecksumException {
		
		InputStream inStream = download(getSessionId(), dataId);
		IOUtils.copy(inStream, destFile);		
	}

	@Override
	public boolean isAvailable(String dataId, Long contentLength, String checksum, FileBrokerArea area) throws JMSException {
		
		try {
			InputStream inStream = download(getSessionId(), dataId);
			IOUtils.closeIfPossible(inStream);
			return true;
		} catch (NotFoundException e) {
			return false;
		}
	}

	
	@Override
	public boolean moveFromCacheToStorage(String dataId) throws JMSException, FileBrokerException {
		return true;
	}
	
	/**
	 * @see fi.csc.microarray.filebroker.FileBrokerClient#getPublicFiles()
	 */
	@Override
	public List<URL> getPublicFiles() throws JMSException {
		return fetchPublicFiles();
	}

	private List<URL> fetchPublicFiles() throws JMSException {
		return new ArrayList<URL>();
	}

	@Override
	public boolean requestDiskSpace(long size) throws JMSException {
		return true;
	}

	@Override
	public void saveRemoteSession(String sessionName, String sessionId, LinkedList<String> dataIds) throws JMSException {
	}

	@Override
	public List<DbSession> listRemoteSessions() throws JMSException {
		
		try {
			HashMap<UUID, fi.csc.chipster.sessiondb.model.Session> sessions;
			sessions = sessionDbClient.getSessions();

			List<DbSession> dbSessions = new LinkedList<>();
			for (fi.csc.chipster.sessiondb.model.Session session : sessions.values()) {
				dbSessions.add(new DbSession(session.getSessionId().toString(), session.getName(), null));
			}

			return dbSessions;
		} catch (RestException e) {
			// the interface shouldn't require JMSExceptions()
			throw new JMSException(Exceptions.getStackTrace(e));
		}
	}
	
	@Override
	public StorageEntryMessageListener getStorageUsage() throws JMSException, InterruptedException {
		return null;
	}

	@Override
	public List<DbSession> listPublicRemoteSessions() throws JMSException {
		return new LinkedList<>();
	}

	@Override
	public void removeRemoteSession(String dataId) throws JMSException {
	}

	@Override
	public String getExternalURL(String dataId) throws JMSException, FileBrokerException, MalformedURLException {
		return null;
	}

	@Override
	public Long getContentLength(String dataId) throws IOException, JMSException, FileBrokerException {
		return null;
	}
}

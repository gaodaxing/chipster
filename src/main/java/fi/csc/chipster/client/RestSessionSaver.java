package fi.csc.chipster.client;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import fi.csc.chipster.rest.RestUtils;
import fi.csc.chipster.sessiondb.SessionDbClient;
import fi.csc.chipster.sessiondb.model.Dataset;
import fi.csc.chipster.sessiondb.model.Input;
import fi.csc.chipster.sessiondb.model.Job;
import fi.csc.chipster.sessiondb.model.Parameter;
import fi.csc.microarray.client.RemoteServiceAccessor;
import fi.csc.microarray.client.Session;
import fi.csc.microarray.client.operation.OperationRecord;
import fi.csc.microarray.client.operation.OperationRecord.InputRecord;
import fi.csc.microarray.client.operation.OperationRecord.ParameterRecord;
import fi.csc.microarray.client.session.SessionManager;
import fi.csc.microarray.client.tasks.Task;
import fi.csc.microarray.client.tasks.Task.State;
import fi.csc.microarray.client.tasks.TaskExecutor;
import fi.csc.microarray.databeans.DataBean;
import fi.csc.microarray.databeans.DataBean.DataNotAvailableHandling;
import fi.csc.microarray.databeans.DataManager;
import fi.csc.microarray.filebroker.ChecksumInputStream;
import fi.csc.microarray.filebroker.FileBrokerClient;
import fi.csc.microarray.messaging.JobState;

public class RestSessionSaver {

	@SuppressWarnings("unused")
	private static final Logger logger = Logger.getLogger(RestSessionSaver.class);
	
	private DataManager 		dataManager 	= Session.getSession().getDataManager();
	private SessionManager 		sessionManager 	= Session.getSession().getApplication().getSessionManager();
	private TaskExecutor 		taskExecutor 	= Session.getSession().getApplication().getTaskExecutor();
	private FileBrokerClient 	fileBroker 		= Session.getSession().getServiceAccessor().getFileBrokerClient();
	private SessionDbClient 	sessionDbClient = ((RemoteServiceAccessor) Session.getSession().getServiceAccessor()).getSessionDbClient();

	public RestSessionSaver() {
	}
	
	public void saveSession() throws Exception{
				
		fi.csc.chipster.sessiondb.model.Session session = new fi.csc.chipster.sessiondb.model.Session();
		session.setName(sessionManager.getSessionName());
		session.setNotes(sessionManager.getSessionNotes());
		session.setCreated(LocalDateTime.now());
		
		UUID sessionId = sessionDbClient.createSession(session);		
		sessionManager.setSessionId(sessionId);

		// create datasets
		HashMap<Dataset, DataBean> datasetToDataBean = new HashMap<>();
		HashMap<String, Dataset> dataIdToDataset = new HashMap<>();
		for (DataBean bean : dataManager.databeans()) {
	
			Dataset dataset = dataBeanToDataset(bean);
			UUID datasetId = sessionDbClient.createDataset(sessionId, dataset);
			dataset.setDatasetId(datasetId);

			ChecksumInputStream inStream = dataManager.getContentStream(bean, DataNotAvailableHandling.EXCEPTION_ON_NA);
			fileBroker.addFile(datasetId.toString(), null, inStream, -1, null);
			
			datasetToDataBean.put(dataset, bean);
			dataIdToDataset.put(bean.getId(), dataset);
		}
		
		for (Dataset dataset : datasetToDataBean.keySet()) {

			// create a job
			DataBean bean = datasetToDataBean.get(dataset);
			OperationRecord record = bean.getOperationRecord();
			Job job = operationRecordToJob(record, null, dataIdToDataset);
			UUID jobId = sessionDbClient.createJob(sessionId, job);
			
			// update dataset's source job
			dataset = sessionDbClient.getDataset(sessionId, dataset.getDatasetId());
			dataset.setSourceJob(jobId);
			sessionDbClient.updateDataset(sessionId, dataset);
		}		
				
		for (Task task : taskExecutor.getTasks(true, true)) {
			Job job = operationRecordToJob(task.getOperationRecord(), task, dataIdToDataset);
			sessionDbClient.createJob(sessionId, job);
		}
	}

	private Dataset dataBeanToDataset(DataBean bean) {
		Dataset dataset = new Dataset();
		dataset.setName(bean.getName());
		dataset.setNotes(bean.getNotes());
		dataset.setX(bean.getY());
		dataset.setY(bean.getX());
		return dataset;
	}

	private Job operationRecordToJob(OperationRecord record, Task task, HashMap<String, Dataset> dataIdToDataset) {
		Job job = new Job();
		job.setStartTime(RestUtils.toLocalDateTime(record.getStartTime()));
		job.setEndTime(RestUtils.toLocalDateTime(record.getEndTime()));
		job.setToolCategory(record.getCategoryName());
		job.setToolDescription(record.getNameID().getDescription());
		job.setToolId(record.getNameID().getID());
		job.setToolName(record.getNameID().getDisplayName());
		job.setModule(record.getModule());
		job.setSourceCode(record.getSourceCode());		
		
		if (task != null) {
			job.setState(taskStateToJobState(task.getState()));
			job.setStateDetail(task.getStateDetail());
			job.setScreenOutput(task.getScreenOutput());
		} else {
			job.setState(JobState.COMPLETED);
		}
		
		List<Input> inputs = new ArrayList<Input>();
		
		for (InputRecord inputRecord : record.getInputRecords()) {
			Input input = new Input();
			Dataset dataset = dataIdToDataset.get(inputRecord.getDataId());
			if (dataset != null && dataset.getDatasetId() != null) {
				input.setDatasetId(dataset.getDatasetId().toString());
			}
			input.setDescription(inputRecord.getNameID().getDescription());
			input.setDisplayName(inputRecord.getNameID().getDisplayName());
			input.setInputId(inputRecord.getNameID().getID());
			inputs.add(input);
		}
		job.setInputs(inputs);
		
		
		List<Parameter> parameters = new ArrayList<Parameter>();
		
		for (ParameterRecord paramRecord : record.getParameters()) {
			Parameter param = new Parameter();
			param.setDescription(paramRecord.getNameID().getDescription());
			param.setDisplayName(paramRecord.getNameID().getDisplayName());
			param.setParameterId(paramRecord.getNameID().getID());
			param.setValue(paramRecord.getValue());
			parameters.add(param);
		}
		job.setParameters(parameters);
		
		return job;
	}

	private JobState taskStateToJobState(State state) {
		switch (state) {
		case CANCELLED:
			return JobState.CANCELLED;			
		case COMPLETED:
			return JobState.COMPLETED;
		case ERROR:
			return JobState.ERROR;
		case FAILED:
			return JobState.FAILED;
		case FAILED_USER_ERROR:
			return JobState.FAILED_USER_ERROR;
		case NEW:
			return JobState.NEW;
		case RUNNING:
			return JobState.RUNNING;
		case TIMEOUT:
			return JobState.TIMEOUT;
		case TRANSFERRING_INPUTS:
			return JobState.RUNNING;
		case TRANSFERRING_OUTPUTS:
			return JobState.RUNNING;
		case WAITING:
			return JobState.WAITING;
		default:
			throw new IllegalArgumentException("unknown task state: " + state);		
		}				
	}
}

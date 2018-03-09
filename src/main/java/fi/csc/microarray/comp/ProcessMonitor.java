package fi.csc.microarray.comp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import fi.csc.microarray.messaging.JobState;
import io.reactivex.subjects.PublishSubject;


public class ProcessMonitor implements Runnable {
	
	public static final int MAX_SCREEN_OUTPUT_SIZE = 100000; // number of chars

	private Process process;
	private Consumer<String> updateScreenOutputCallback;
	private BiConsumer<JobState, String> finishCallback;
	private Pattern successStringPattern;
	private CountDownLatch finishedLatch;

	
	private PublishSubject<Boolean> notifyClientSubject = PublishSubject.create();

	private StringBuffer screenOutput = new StringBuffer();
	
	static final Logger logger = Logger.getLogger(ProcessMonitor.class);
	
	public ProcessMonitor(
			Process process, 
			Consumer<String> updateScreenOutputCallback,
			BiConsumer<JobState, String> finishCallback,
			Pattern successStringPattern,
			CountDownLatch finishedLatch) {
		this.process = process;
		this.updateScreenOutputCallback = updateScreenOutputCallback;
		this.finishCallback = finishCallback;
		this.successStringPattern = successStringPattern;

		this.finishedLatch = finishedLatch; // FIXME remove from here
	}

	public void run() {
		
		// FIXME just stop if job has been cancelled
		logger.debug("process monitor started");

		// subscribe to and throttle send output notification stream
		notifyClientSubject
		.throttleLast(1, TimeUnit.SECONDS)
		.subscribe(arg -> {
			updateScreenOutputCallback.accept(screenOutput.toString());
		});

		
		// read process output stream
		BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
		boolean readMore = true;
		try {
			for (String line = reader.readLine(); readMore ; line = reader.readLine()) {
				
				// read end of stream --> error
				if (line == null) {
					finishCallback.accept(JobState.FAILED, screenOutput.toString());
					readMore = false;
				} 
				
				// read script successful
				else if (successStringPattern.matcher(line).matches()) {
					screenOutput.append(line + "\n");
					finishCallback.accept(JobState.RUNNING, screenOutput.toString());
					readMore = false;
				}
				
				// read normal output
				else {
//					// exculde print success string command
//					if (line.contains(CompJob.SCRIPT_SUCCESSFUL_STRING)) {
//						continue;
//					}
					
					// make sure it always ends with \n
					line = line + "\n";
					
					// enough space in the buffer
					if (screenOutput.length() + line.length() <= MAX_SCREEN_OUTPUT_SIZE) {
						screenOutput.append(line);
						notifyClientSubject.onNext(true);
					} 

//					// FIXME
//					// buffer not full but not enough space for the whole line
//					else if (screenOutput.length() < MAX_SCREEN_OUTPUT_SIZE) {
//						screenOutput.append(line.substring(0, MAX_SCREEN_OUTPUT_SIZE - screenOutput.length()));
//						readMore = false;
//					} else if (screenOutput.length() >= MAX_SCREEN_OUTPUT_SIZE) {
//						readMore = false;
//						
//					} 
				}
			}
		} catch (IOException e) {
			// also canceling the job leads here 
			finishCallback.accept(JobState.ERROR, screenOutput.toString());
		} finally {
			notifyClientSubject.onComplete();
		}

		finishedLatch.countDown();
	}
			
	public String getOutput() {
		return screenOutput.toString();
	}
}

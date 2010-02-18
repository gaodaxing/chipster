package fi.csc.microarray.client.visualisation.methods.gbrowser.dataFetcher;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import fi.csc.microarray.client.visualisation.methods.gbrowser.fileFormat.FileParser;
import fi.csc.microarray.client.visualisation.methods.gbrowser.message.FileRequest;
import fi.csc.microarray.client.visualisation.methods.gbrowser.message.FileResult;


public class FileFetcherThread extends Thread{
				
	private BlockingQueue<FileRequest> fileRequestQueue;
	private ConcurrentLinkedQueue<FileResult> fileResultQueue;
	
	private TreeThread treeThread;
	
	private RandomAccessFile raf;
	private FileParser inputParser;

	public FileFetcherThread(BlockingQueue<FileRequest> fileRequestQueue,
			ConcurrentLinkedQueue<FileResult> fileResultQueue, TreeThread treeThread, 
			FileParser inputParser) {
		
		this.fileRequestQueue = fileRequestQueue;
		this.fileResultQueue = fileResultQueue;
		this.treeThread = treeThread;
		this.inputParser = inputParser;
		
		this.setDaemon(true);
		
		try {
			raf = new RandomAccessFile(treeThread.getFile(), "r");
			//raf = new RandomAccessFile(new File("bowtie.fsf"), "r");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		while(true){
			try {
				
				processFileRequest(fileRequestQueue.take());
				
//				if(fileRequestQueue.peek() != null){
//					processFileRequest(fileRequestQueue.poll());					
//				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
					e.printStackTrace();
			}						
		}
	}

	private void processFileRequest(FileRequest fileRequest) throws IOException {
		
		if ( !fileRequest.status.file.equals(treeThread.getFile())) {
			System.out.println("FileFetcherProcess: " + fileRequest.status.file + ", " + treeThread.getFile() + ", " + inputParser);
		}
		
//		System.out.println("File: Got file request " + fileRequest.region.start);
		
		ByteChunk chunk = new ByteChunk(inputParser.getChunkMaxByteLength());			
		chunk.rowIndex = fileRequest.rowRegion.start;
		
		raf.seek(inputParser.getFilePosition(chunk.rowIndex));
		
		chunk.byteLength = raf.read(chunk.byteContent);
		
		fileRequest.status.maybeClearQueue(fileResultQueue);
		fileRequest.status.fileRequestCount = fileRequestQueue.size();
		
		FileParser inputParser = (FileParser) this.inputParser.clone();
		inputParser.setChunk(chunk);
		
		fileResultQueue.add(new FileResult(fileRequest, inputParser, fileRequest.status));
		treeThread.notifyTree();
		
	}

	public long getRowCount() {
		if(this.isAlive()){
			throw new IllegalStateException("Must be called before the thread is started");
		}
		
		try {
			return inputParser.getRowIndex(raf.length());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}
}

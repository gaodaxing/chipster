package fi.csc.microarray.filebroker;

import java.net.MalformedURLException;
import java.net.URL;

import fi.csc.microarray.config.Configuration;

// FIXME obsolete, should be removed
public class FileBrokerConfig {
	@Deprecated
	public static URL generateUrlToSomeFileBroker(String filename) throws MalformedURLException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Returns port for this file broker instance. Should be used only by filebrokers.
	 */
	public static int getPort() {
		return Integer.parseInt(Configuration.getValue("filebroker", "port"));
	}
}

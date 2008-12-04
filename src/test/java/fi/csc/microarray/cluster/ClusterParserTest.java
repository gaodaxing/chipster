package fi.csc.microarray.cluster;

import org.testng.annotations.Test;

public class ClusterParserTest {

	private static String basicTree = "(((211357_s_at:0.0085309292,((207407_x_at:0.0005297675,217319_x_at:0.0005297675):0.0042627223," +
			"((205719_s_at:0.0005884716,217238_s_at:0.0005884716):0.0017966716,(204705_x_at:0.0007526250," +
			"208383_s_at:0.0007526250):0.0016325182):0.0024073465):0.0037384395):0.0122482037,((220281_at:" +
			"0.0033785216,(206054_at:0.0013147940,206716_at:0.0013147940):0.0020637276):0.0070371419," +
			"(206024_at:0.0073043209,((205625_s_at:0.0015355018,(204704_s_at:0.0007322246,205626_s_at:" +
			"0.0007322246):0.0008032772):0.0020128110,(205892_s_at:0.0021425591,211298_s_at:0.0021425591):" +
			"0.0014057537):0.0037560081):0.0031113426):0.0103634694):0.1167723057,((205030_at:0.0174587795," +
			"(218484_at:0.0009559207,(206025_s_at:0.0004036710,213479_at:0.0004036710):0.0005522497):" +
			"0.0165028588):0.0453366933,((211756_at:0.0091295527,(201650_at:0.0043539003,218545_at:" +
			"0.0043539003):0.0047756524):0.0216270861,((202286_s_at:0.0037701646,(201744_s_at:0.0001912771," +
			"204259_at:0.0001912771):0.0035788875):0.0200289757,((202018_s_at:0.0038957269,203021_at:" +
			"0.0038957269):0.0069633756,((202310_s_at:0.0010940080,(202404_s_at:0.0006089324,221731_x_at:" +
			"0.0006089324):0.0004850756):0.0047416419,(201909_at:0.0021842264,221872_at:0.0021842264):" +
			"0.0036514235):0.0050234525):0.0129400378):0.0069574985):0.0320388341):0.0747559658);";
	
	private static String geneNamesWithNumbers = "(211357:0.0085309292,211357:0.0085309292);";
	
	@Test(groups = {"unit"} )
	public void test() throws TreeParseException {
		new ClusterParser(basicTree).getTree();
		new ClusterParser(geneNamesWithNumbers).getTree();
	}
	
	public static void main(String[] args) throws TreeParseException {
		new ClusterParserTest().test();
		System.out.println("alles ok");
	}
}

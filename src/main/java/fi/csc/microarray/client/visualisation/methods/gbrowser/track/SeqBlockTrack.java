package fi.csc.microarray.client.visualisation.methods.gbrowser.track;

import java.awt.Color;
import java.awt.Rectangle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import fi.csc.microarray.client.visualisation.methods.gbrowser.DataSource;
import fi.csc.microarray.client.visualisation.methods.gbrowser.View;
import fi.csc.microarray.client.visualisation.methods.gbrowser.dataFetcher.AreaRequestHandler;
import fi.csc.microarray.client.visualisation.methods.gbrowser.drawable.Drawable;
import fi.csc.microarray.client.visualisation.methods.gbrowser.drawable.RectDrawable;
import fi.csc.microarray.client.visualisation.methods.gbrowser.fileFormat.ColumnType;
import fi.csc.microarray.client.visualisation.methods.gbrowser.fileFormat.Strand;
import fi.csc.microarray.client.visualisation.methods.gbrowser.message.AreaResult;
import fi.csc.microarray.client.visualisation.methods.gbrowser.message.BpCoord;
import fi.csc.microarray.client.visualisation.methods.gbrowser.message.RegionContent;

/**
 * Track that shows actual content of reads. At high level, values of nucleotides are not shown. At low level, they are
 * shown.
 *
 */
public class SeqBlockTrack extends Track {

	private static final int MAX_STACKING_DEPTH = 10;

	public static final String DUMMY_SEQUENCE = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
								+ "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
								+ "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

	private static final int SPACE_BETWEEN_READS = 2;

	private static final Color[] charColors = new Color[] {
			new Color(64, 192, 64, 128), // A
			new Color(64, 64, 192, 128), // C
			new Color(128, 128, 128, 128), // G
			new Color(192, 64, 64, 128) // T
	};

	private Collection<RegionContent> reads = new TreeSet<RegionContent>();
	private List<Integer> occupiedSpace = new ArrayList<Integer>();

	private long maxBpLength;
	private long minBpLength;

	private boolean wasLastConsised = true;

	public SeqBlockTrack(View view, DataSource file, 
			Class<? extends AreaRequestHandler> handler,
			Color fontColor, long minBpLength,
			long maxBpLength) {
		super(view, file, handler);
		this.minBpLength = minBpLength;
		this.maxBpLength = maxBpLength;
	}

	@Override
	public Collection<Drawable> getDrawables() {
		Collection<Drawable> drawables = getEmptyDrawCollection();

		occupiedSpace.clear();

		// iterate over RegionContent objects (one object corresponds to one read)
		if (reads != null) {
			Iterator<RegionContent> iter = reads.iterator();

			while (iter.hasNext()) {

				RegionContent read = iter.next();

				// remove those that are not in this view
				if (!read.region.intercepts(getView().getBpRegion())) {
					iter.remove();
					continue;
				}

				// collect relevant data for this read
				Color blockColor = Color.gray;
				int height = 2; // (int) (getView().getTrackHeight() / 2 * limited / 255f);
				BpCoord startBp = read.region.start;
				BpCoord endBp = read.region.end;
				String seq = ((String) read.values.get(ColumnType.SEQUENCE));

				if (seq != null) {
					// we have sequence
					seq = seq.trim();
					
				} else {
					// we don't have a sequence, generate something
					int seqLength = (int) (endBp.minus(startBp) + 1);
					
					if (seqLength > 128) {
						// TODO what happened?
						seqLength = 0;
					}
					
					seq = DUMMY_SEQUENCE.substring(0, seqLength);
				}

				// create rectangle covering the correct screen area (x-axis)
				Rectangle rect = new Rectangle();
				rect.x = getView().bpToTrack(startBp);
				rect.width = getView().bpToTrack(endBp) - rect.x;

				// reads are drawn in order and placed in layers
				int layer = 0;
				while (occupiedSpace.size() > layer
						&& occupiedSpace.get(layer) > rect.x + 1) {
					layer++;
				}

				// read reserves the space of the layer from end to left corner of the screen
				int end = rect.x + rect.width;
				if (occupiedSpace.size() > layer) {
					occupiedSpace.set(layer, end);
				} else {
					occupiedSpace.add(end);
				}

				// check maximum stacking depth
				if (layer > MAX_STACKING_DEPTH) {
					continue; 
				}
				
				// now we can decide the y-axis
				rect.y = (int) (getView().getTrackHeight() - ((layer + 1) * (height + SPACE_BETWEEN_READS)));
				rect.height = height;

				// reverse the read if on reverse strand
				if ((Strand) read.values.get(ColumnType.STRAND) == Strand.REVERSED) {
					StringBuffer buf = new StringBuffer(seq).reverse();
					seq = buf.toString();
				}

				
				if (rect.width < seq.length()) {
					
					Color color = blockColor;
					// mark last line that will be drawn
					if (layer == MAX_STACKING_DEPTH) {
						color = color.darker();
					}

					// no space to draw actual sequence
					drawables.add(new RectDrawable(rect, color, null));

				} else { 
					// enough space to show the actual sequence

					// draw arrow
					if (read.values.get(ColumnType.STRAND) == Strand.REVERSED) {
						drawables.addAll(getArrowDrawables(rect.x, rect.y,
								-rect.height, rect.height));
					} else {
						drawables.addAll(getArrowDrawables(rect.x + rect.width,
								rect.y, rect.height, rect.height));
					}

					// prepare to draw single nucleotides
					float x = rect.x;
					float increment = (rect.width) / ((float) seq.length());

					// draw something for each nucleotide
					for (int j = 0; j < seq.length(); j++) {

						char letter = seq.charAt(j);

						// character drawing commented away:
						//
						// if (rect.width > seq.length() * CHAR_WIDTH) {
						//
						// drawables.add(new TextDrawable((int) x + 1, rect.y +
						// 10, "" + letter, fontColor));
						//
						// // drawables.add(new TextDrawable((int)x, rect.y - 8,
						// // "" + letter, Color.black));
						//
						// }

						Color bg = Color.white;
						if (letter == 'A') {
							bg = charColors[0];
						} else if (letter == 'C') {
							bg = charColors[1];
						} else if (letter == 'G') {
							bg = charColors[2];
						} else if (letter == 'T') {
							bg = charColors[3];
						}
						
						// mark last line that will be drawn
						if (layer == MAX_STACKING_DEPTH) {
							bg = bg.brighter();
						}

						drawables.add(new RectDrawable((int) x + 1, rect.y,
								(int) increment, height, bg, null));

						x += increment;
					}
				}
			}
		}

		return drawables;
	}

	public void processAreaResult(AreaResult<RegionContent> areaResult) {

		// check that areaResult has same concised status (currently always false)
		// and correct strand
		if (areaResult.status.concise == isConcised()
				&& areaResult.content.values.get(ColumnType.STRAND) == getStrand()) {
			
			// add this to queue of RegionContents to be processed
			this.reads.add(areaResult.content);
			getView().redraw();
		}
	}

	@Override
	public void updateData() {

		if (wasLastConsised != isConcised()) {
			reads.clear();
			wasLastConsised = isConcised();
		}
		super.updateData();
	}

    @Override
    public Integer getHeight() {
        if (isVisible()) {
            return super.getHeight();
        } else {
            return 0;
        }
    }
    
    @Override
    public boolean isStretchable() {
        // stretchable unless hidden
        return isVisible();
    }
    
    @Override
    public boolean isVisible() {
        // visible region is not suitable
        return (super.isVisible() &&
                getView().getBpRegion().getLength() > minBpLength &&
                getView().getBpRegion().getLength() <= maxBpLength);
    }

	@Override
	public Collection<ColumnType> getDefaultContents() {
		return Arrays.asList(new ColumnType[] { ColumnType.SEQUENCE,
				ColumnType.STRAND });
	}

	@Override
	public boolean isConcised() {
		return false;
	}
}

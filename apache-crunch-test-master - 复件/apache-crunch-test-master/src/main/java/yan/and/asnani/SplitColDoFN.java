package yan.and.asnani;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

public class SplitColDoFN extends DoFn<String, String> {

	private Integer column;

	public SplitColDoFN(Integer column) {
		super();
		this.column = column-1;
	}

	public Integer getColumn() {
		return column;
	}

	@Override
	public void process(String line, Emitter<String> emitter) {
		if (!"".equals(line)) {
			String[] lineSplit = line.split(";");
			if (lineSplit.length == 2) {
				emitter.emit(lineSplit[getColumn()]);
			}else {
				emitter.emit(line);
			}
		}

	}

}

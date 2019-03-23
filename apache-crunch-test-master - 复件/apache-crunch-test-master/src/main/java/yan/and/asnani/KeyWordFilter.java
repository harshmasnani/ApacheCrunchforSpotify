package yan.and.asnani;

import org.apache.crunch.FilterFn;

public class KeyWordFilter extends FilterFn<String> {

	private String startedWith;

	public KeyWordFilter(String startedWith) {
		super();
		this.startedWith = startedWith;
	}
	
	public String getStartedWith() {
		return startedWith;
	}

	@Override
	public boolean accept(String input) {
		return input.startsWith(getStartedWith());

	}
}

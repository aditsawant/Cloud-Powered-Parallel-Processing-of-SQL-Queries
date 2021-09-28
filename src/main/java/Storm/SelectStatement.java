import backtype.storm.tuple.Values;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class SelectStatement extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String[] newFields = tuple.GetFields();
        collector.emit(new Values(tuple.select(newFields)));
    }
}
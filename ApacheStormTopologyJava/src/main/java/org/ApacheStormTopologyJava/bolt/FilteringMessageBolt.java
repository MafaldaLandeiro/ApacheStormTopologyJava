package org.ApacheStormTopologyJava.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilteringMessageBolt extends BaseBasicBolt {

    private static final Logger log = LoggerFactory
            .getLogger(FilteringMessageBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String randomMessage = tuple.getStringByField("randomMessage");
        if (!randomMessage.contains("z")) {
            basicOutputCollector.emit(tuple.getValues());
        } else {
            log.info("Message filter out: " + randomMessage);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("randomMessageFiltered", "timestamp"));
    }
}

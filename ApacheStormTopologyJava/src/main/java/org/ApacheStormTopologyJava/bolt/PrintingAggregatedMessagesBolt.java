package org.ApacheStormTopologyJava.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintingAggregatedMessagesBolt extends BaseBasicBolt {

    private static final Logger log = LoggerFactory
            .getLogger(PrintingAggregatedMessagesBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String randomMessagesConcatenate = tuple.getStringByField("randomMessagesConcatenate");
        log.info("Random Messages Concatenate: " + randomMessagesConcatenate);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

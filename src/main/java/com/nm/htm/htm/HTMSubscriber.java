package com.nm.htm.htm;

import java.util.ArrayList;
import java.util.List;

import org.numenta.nupic.network.Inference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Subscriber;

public class HTMSubscriber extends Subscriber<Inference> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HTMSubscriber.class);
    
    private List<ResultState> states = new ArrayList<>();
    
    @Override
    public void onCompleted() {
        LOGGER.debug("HTM completed");
    }

    @Override
    public void onError(Throwable e) {
        LOGGER.error("Error: ", e);
    }

    @Override
    public void onNext(Inference inference) {
        int recordNumber = inference.getRecordNum();
        double prediction = getPrediction();
        double predictionNext = (double) inference.getClassification(HTMNetwork.STR_MEASUREMENT).getMostProbableValue(1);
        double actual = (double) inference.getClassifierInput().get(HTMNetwork.STR_MEASUREMENT).get("inputValue");
        double error = 0.0d, anomaly = 0.0d;
        if (recordNumber > 0) {
            error = Math.abs(prediction - actual);
            anomaly = inference.getAnomalyScore();
        }
        states.add(new ResultState(recordNumber, null, actual, prediction, error, anomaly, predictionNext));
    }
    
    private double getPrediction() {
        double result = .0;
        if (states != null && !states.isEmpty()) {
            result = states.get(states.size() - 1).getPredictionNext();
        }
        return result;
    }

    public List<ResultState> getStates() {
        return states;
    }

    public void setStates(List<ResultState> states) {
        this.states = states;
    }
    
}

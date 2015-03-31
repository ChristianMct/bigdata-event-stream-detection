package org.epfl.bigdataevs;

import java.io.Serializable;
import java.util.Map;

import org.epfl.bigdataevs.eminput.TimePeriod;


public class Theme {

    public Map<String, Integer> words_distribution;
    public TimePeriod time_span;
	
    public Theme(Map<String, Integer> words_distribution, TimePeriod time_span){
	this.words_distribution = words_distribution;
	this.time_span = time_span;
    }
}

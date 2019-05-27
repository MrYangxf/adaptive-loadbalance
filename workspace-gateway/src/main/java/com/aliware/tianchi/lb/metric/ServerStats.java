package com.aliware.tianchi.lb.metric;

import java.io.Serializable;

/**
 * todo: 
 * @author yangxf
 */
public class ServerStats implements Serializable {
    private static final long serialVersionUID = -7372381913642867174L;
    
    private float la01;
    private float la05;
    private float la15;
    
    private float cpuRate;
    private float usRate;
    private float syRate;
    
    private float memRate;
    private float ioRate;
    
}

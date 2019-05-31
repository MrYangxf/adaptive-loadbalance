package com.aliware.tianchi.common.util;

/**
 * @author yangxf
 */
public enum RunState {
    IDLE {
        public double threshold() {
            return .3d;
        }
    },
    
    RULY {
        public double threshold() {
            return .55d;
        }
    },
    
    BUSY {
        public double threshold() {
            return .75d;
        }
    },
    
    OVER {
        public double threshold() {
            return .90d;
        }
    };

    public abstract double threshold();
}

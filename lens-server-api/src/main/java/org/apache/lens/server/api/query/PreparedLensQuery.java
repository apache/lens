package org.apache.lens.server.api.query;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
public class PreparedLensQuery {

    @Getter
    @Setter
    private String handle;
    
    @Getter
    @Setter
    private String userquery;
    
    @Getter
    @Setter
    private String submitter;
    
    @Getter
    @Setter
    private long timetaken;
    
    @Getter
    @Setter
    private String queryname;
    
    @Getter
    @Setter
    
    private String drivername;
    @Getter
    @Setter
    
    private String driverquery;
    
    @Getter
    @Setter
    private long starttime;
}

package com.lf.xcode.rds.task.rdsbatch.param;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * 执行参数
 */
@Getter
@Setter
@ToString
@Accessors(chain = true)
public class InvokeParam {

    private String mainTask;
    private String subTask;
}

package com.yuxin.kvlsmtree.model.command;

import lombok.Getter;
import lombok.Setter;
import com.alibaba.fastjson.JSON;

@Getter
@Setter
public abstract class AbstractCommand implements Command {
    private CommandTypeEnum type;

    public AbstractCommand(CommandTypeEnum type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

}

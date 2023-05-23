package com.summer.kvstore.utils;

import com.alibaba.fastjson.JSONObject;
import com.summer.kvstore.model.command.Command;
import com.summer.kvstore.model.command.CommandTypeEnum;
import com.summer.kvstore.model.command.RmCommand;
import com.summer.kvstore.model.command.SetCommand;

/**
 * 领域模型转换工具类
 */
public class ConvertUtil {

    public static final String TYPE = "type";

    public static Command jsonToCommand(JSONObject value) {
        if (value.getString(TYPE).equals(CommandTypeEnum.SET.name())) {
            return value.toJavaObject(SetCommand.class);
        } else if (value.getString(TYPE).equals(CommandTypeEnum.RM.name())) {
            return value.toJavaObject(RmCommand.class);
        }
        return null;
    }
}

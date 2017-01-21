package lwwl.bbtt.json2db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.springframework.batch.item.file.LineMapper;

/**
 * Created by awul on 2017/1/15.
 */
public class JsonLineMapper implements LineMapper<JSONObject> {

    @Override
    public JSONObject mapLine(String line, int lineNumber) throws Exception {
        JSONObject obj = JSON.parseObject(line);
        Object id = obj.getJSONObject("_id").get("$oid");
//        Object id = JSONPath.eval(obj, "$._id.$oid");
        obj.put("sourceid", id);
        obj.put("sourcetype", "1");
        obj.put("sourcedoc", line);
        return obj;
    }

}

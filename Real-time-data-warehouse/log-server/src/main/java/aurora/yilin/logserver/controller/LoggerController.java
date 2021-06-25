package aurora.yilin.logserver.controller;

import aurora.yilin.Constant.KafkaConstant;
import aurora.yilin.utils.PropertiesAnalysisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.Properties;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/25
 */
@RestController
public class LoggerController {

    private static final Logger log = LoggerFactory.getLogger(LoggerController.class);

    @Resource
    KafkaTemplate<String,String> kafkaTemplate;

    @RequestMapping("/applog")
    public String getLogger(@RequestParam("param")String jsonLogger){
        kafkaTemplate.send(PropertiesAnalysisUtil.getInfoBykeyFromPro(KafkaConstant.OSD_LOG_TOPIC.getValue()),jsonLogger);


        log.info(jsonLogger);
        return "success";
    }
}

package com.sl.connector.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.sl.connector.constant.CommonConstant.MODULE_VERSION;

/**
 * @author L
 */
public class ModuleUtils {
    private static final Logger logger = LoggerFactory.getLogger(ModuleUtils.class);

    public static String version(String buildFile) {
        InputStream stream = ModuleUtils.class.getClassLoader().getResourceAsStream(buildFile);
        try {
            Properties props = new Properties();
            props.load(stream);
            return props.getProperty(MODULE_VERSION);
        } catch (IOException e) {
            logger.error("加载module文件: {}, 失败. ", buildFile, e);
            throw new IllegalArgumentException("加载module文件: " + buildFile + ", 失败");
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

}

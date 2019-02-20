import com.sl.connector.utils.ModuleUtils;
import org.junit.Test;

public class ModuleUtilsUtils {

    @Test
    public void versionTest() {
        String version = ModuleUtils.version("build.properties");
        System.out.println(version);
    }
}

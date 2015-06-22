
import com.hazelcast.jet.api.CombinedJetException;
import com.hazelcast.jet.api.config.JetConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.experimental.categories.Category;

import java.util.function.Consumer;
import java.util.stream.Stream;

import com.hazelcast.core.IList;
import com.hazelcast.config.Config;

import java.util.function.Predicate;

import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.jet.impl.hazelcast.JetHazelcast;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.jet.api.hazelcast.JetHazelcastInstance;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ListFilterTest {
    @Test
    public void test() throws Exception {
        JetHazelcastInstance instance = JetHazelcast.newHazelcastInstance(new JetConfig());
        IList<String> list = instance.getList("list");

        for (int i = 1; i < 100; i++) {
            list.add("b" + i);
            list.add("a" + i);
            list.add("c" + i);
        }

        try {
            list.stream()
                    .filter(new Predicate<String>() {
                        @Override
                        public boolean test(String s) {
                            return s.startsWith("a");
                        }
                    })
                    .sorted()
                    .forEach(new Consumer<String>() {
                        @Override
                        public void accept(String s) {
                            System.out.println("Result=" + s);
                        }
                    });
        } catch (CombinedJetException exception) {
            for (Throwable t : exception.getErrors()) {
                t.printStackTrace(System.out);
            }
        }
    }
}

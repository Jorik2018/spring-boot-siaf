package org.isobit.siaf;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SiafWebApplication {

	public static void main(String[] args) {
		System.setProperty("java.io.tmpdir", "C:\\Windows\\Temp");
		new SpringApplicationBuilder(SiafWebApplication.class)
        .properties("spring.config.name:siaf-web").build().run(args);
	}

}

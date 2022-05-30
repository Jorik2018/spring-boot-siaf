package gob.regionancash.siaf;

import org.isobit.siaf.service.SiafService;
import org.junit.jupiter.api.Test;
//import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootTest
//@RunWith(SpringRunner.class)
class SiafWebApplicationTests {
	
	//@Autowired
	//private SiafService siafService;

	
	@Test
	void contextLoads() throws JsonProcessingException {
		
		
		System.out.println("hola");
		
		
		/*System.out.println(
		new ObjectMapper()*
		.writeValueAsString(siafService.getResultList(6, 0, new Object[]{"20530592520"})));*/
	}
	
	

}

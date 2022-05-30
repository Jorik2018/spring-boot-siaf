package org.isobit.siaf.controller;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceContext;

import org.isobit.siaf.dto.Renta5Form;
import org.isobit.siaf.jpa.HCC;
import org.isobit.siaf.repository.HCCRepository;
import org.isobit.siaf.service.SiafService;
import org.isobit.util.XUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

@RestController
@RequestMapping(value = "")
public class SiafRestController {
	
	@Autowired
	private SiafService siafService;
	
    @Autowired
    private HCCRepository hccRepository;

	@CrossOrigin
	@PostMapping(value="/{id}/{ano_eje}/{expediente}",consumes = MediaType.APPLICATION_JSON_VALUE)
	public Object report2(
			@PathVariable("id") int id,
			@PathVariable("ano_eje") int ano_eje,
			@PathVariable("expediente") String[] expediente,@RequestBody Map map) throws IOException {
		System.out.println(map);
		Object o=siafService.getResultList(id,ano_eje,expediente,map);
		//Solicito generar un archivo temporal y que me envien el nombre para poder referenciarlo
		if(map.containsKey("file")) {
			HashMap m=new HashMap();
			File tempFile = File.createTempFile("___", new Date().getTime()+".json");
			new ObjectMapper().writeValue(tempFile, o);
			m.put("tempFile", tempFile.getName());
			return m;
		}else 
			return o;
	}
	
	
	@CrossOrigin
	@GetMapping("/expediente-x-proveedor/{ano_eje}/{ruc}")
	public Object listExpedienteByProvider(
			@PathVariable("ano_eje") int ano_eje,
			@PathVariable("ruc") String ruc
			) {
		HashMap m=new HashMap();
		m.put("ruc",ruc);
		return siafService.getResultList(5,ano_eje,null,m);
	}
	
	@CrossOrigin
	@GetMapping("/expediente-x-meta/{ano_eje}/{secFunc}")
	public Object listExpedienteByMeta(
			@PathVariable("ano_eje") int ano_eje,
			@PathVariable("secFunc") int secFunc
			) {
		HashMap m=new HashMap();
		m.put("secFunc",secFunc);
		return siafService.getResultList(5,ano_eje,null,m);
	}
	
	@CrossOrigin
	@GetMapping("/5/{ano_eje}/{from}/{to}")
	public Object list(
			@PathVariable("ano_eje") int ano_eje,
			@PathVariable("from") int from,
			@PathVariable("to") int to,
			@RequestParam(required = false) Integer secFunc,
			@RequestParam(required = false) String provider,
			@RequestParam(required = false) String purpose,
			@RequestParam(required = false) Integer month,
			@RequestParam(required = false) String ruc
			) {
		HashMap m=new HashMap();
		m.put("from",from);
		m.put("to",to);
		if(secFunc!=null)m.put("secFunc",secFunc);
		if(provider!=null)m.put("provider",provider);
		if(purpose!=null)m.put("purpose",purpose);
		if(month!=null)m.put("month",month);
		if(ruc!=null)m.put("ruc",ruc);
		return siafService.getResultList(5,ano_eje,null,m);
	}

	@CrossOrigin
	@GetMapping("/{id}/{ano_eje}/{expediente}")
	public Object report(
			@PathVariable("id") int id,
			@PathVariable("ano_eje") int ano_eje,
			@PathVariable("expediente") String[] expediente) {
		return siafService.getResultList(id,ano_eje,expediente);
	}
	
	@CrossOrigin
	@GetMapping("/hcc/{from}/{to}")
	public Object report(@PathVariable("from") int from,
			@PathVariable("to") int to) {
		return hccRepository.findAll();
	}
		
	@PersistenceContext
    private EntityManager em;

	@CrossOrigin
	@GetMapping("/hcc/{id}")
	public Object get(@PathVariable("id") int id) {
		HCC hcc=hccRepository.findById(id).get();
		hcc.setExt(this.report(2, hcc.getAnoEje(),new String[] {""+hcc.getExpediente()}));
		return hcc;
	}
	  
	@CrossOrigin
	@PostMapping("/hcc")
	public Object postHCC(@RequestBody HCC hcc) {
		if(hcc.getNumber()==null) {
			HCC hcc0=null;
			try {
			hcc0=em.createQuery("SELECT u FROM HCC u where u.anoEje=?1 AND u.month = ?2 AND u.number>0 ORDER BY u.number DESC",HCC.class)
					.setParameter(1, hcc.getAnoEje())
					.setParameter(2, hcc.getMonth())
					.setMaxResults(1).getSingleResult();
			
			}catch(NoResultException ex) {
				
			}
			hcc.setNumber(hcc0!=null?hcc0.getNumber()+1:1);
		}else {
			//Se actualizara el registro de numero de hcc seleccionado con los datos del expediente seleccionado 
			HCC hcc0=em.createQuery("SELECT u FROM HCC u where u.anoEje=?1 AND u.month = ?2 AND u.number=?3 ORDER BY u.number DESC",HCC.class)
					.setParameter(1, hcc.getAnoEje())
					.setParameter(2, hcc.getMonth())
					.setParameter(3, hcc.getNumber())
					.setMaxResults(1).getSingleResult();
			hcc.setId(hcc0.getId());
		}
		if(hcc.getCreation()==null)hcc.setCreation(new Date());
		return hccRepository.save(hcc);
	}
	
	@CrossOrigin
	@GetMapping("/sync")
	public void sync() {
		try {
			siafService.synkk(0);
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@CrossOrigin
	@GetMapping("/sync/{id}")
	public void sync(@PathVariable("id") int year) {
		try {
		siafService.synkk(year);
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
	

	@CrossOrigin
	@PostMapping("/renta5")
	public Object download(@RequestBody Renta5Form m) throws IOException {
		
		RestTemplate restTemplate = new RestTemplate();
		String fileName=m.tempFile;
		Integer tab=m.tab;
		Integer month=m.month;
		//http://web.regionancash.gob.pe
		File file = restTemplate.execute("http://localhost/xls/api/jao/"+fileName
				+(tab!=null?"":"?with-names=true"), HttpMethod.POST, 
				null, clientHttpResponse -> {
		    File ret = File.createTempFile("download", "tmp");
		    StreamUtils.copy(clientHttpResponse.getBody(), new FileOutputStream(ret));
		    return ret;
		});
		Object dd=readObject(file);
        
		if(tab==null){
        	HashMap m2=new HashMap();
        	m2.put("pages",((Object[])dd)[0]);
        	m2.put("tempFile", fileName);
        	return m2;
        }
		List[] pages=(List[]) dd;
        List<Object[]> s = pages[tab];
        String ss="";
        try {
            //Store s = (Store) Traslator.get(XFile.getFileExtension(f)).read(f, new Store(new Q()).setDefault("tab", tab).setDefault("C2", BigDecimal.class));
            System.out.println("s.getRowCount()=" + s.size());

            //ArrayList s2 = new ArrayList();
            
            SimpleDateFormat fo = new SimpleDateFormat("dd-MMM-yyyy");
            SimpleDateFormat fo2 = new SimpleDateFormat("dd/MM/yyyy");

            for (int i = 2; i < s.size(); i++) {
                if (XUtil.intValue(s.get(i)[0]) == 0) {
                    continue;
                }
                try {
                	long l=new BigDecimal(s.get(i)[2].toString()).longValue();
                	s.get(i)[2]=String.format("%08d",l);
                }catch(Exception e) {
                	System.out.println(i);
                }
                Object row[] = new Object[]{
                    6, //"tipo_doc",_SIZE, 2,_CLASS, String.class
                    s.get(i)[2], //"numero_doc", _SIZE, 15, _CLASS, String.class
                    'R', //"tipo_comemi", _SIZE, 1, _CLASS, String.class
                    null, //"serie", Store._SIZE, 4, _CLASS, String.class
                    null, //"numero", Store._SIZE, 8, _CLASS, String.class
                    s.get(i)[10],//"monto",_CLASS, Double.class
                    null, //"fecha_emision",_CLASS, String.class
                    null, //"fecha_pago",_CLASS, String.class
                    !XUtil.isEmpty(s.get(i)[11]) ? 1 : 0, //"retencion",_SIZE, 1,_CLASS, String.class 
                    "", //"a",_CLASS, String.class
                    "", //"b",_CLASS, String.class
                    "" //"c",_CLASS, String.class
                };
                String w = (String) s.get(i)[4];
                if (w != null) {
                    String w2[] = w.split("-");
                    if (w2.length > 0) {
                        row[3] = w2[0].trim();
                        if (w2.length > 1) {
                            row[4] = w2[1].trim();
                        }
                    }
                }
                try {
                    row[6] = fo2.format(s.get(i)[5]);
                } catch (Exception ee) {
                    ee.printStackTrace();
                }
                try {
                    row[7] = fo2.format(s.get(i)[7]);
                } catch (Exception ee) {
                    ee.printStackTrace();
                }
                ss+=XUtil.implode(row,"|")
                		//+"\r\n"
                		+System.lineSeparator();
            }
            fo2 = new SimpleDateFormat("yyyyMM");
            Calendar calendar=Calendar.getInstance();
            calendar.set(Calendar.MONTH,month);
    	    return ResponseEntity.ok()
    	            .header("content-disposition", "attachment; filename = " + "0601" + fo2.format(calendar.getTime()) + "20148309109.4ta")
    	            //.contentLength(file.length())
    	            .contentType(MediaType.APPLICATION_OCTET_STREAM)
    	            .body(new InputStreamResource(new ByteArrayInputStream(ss.getBytes())))
    	           // .body(new InputStreamResource(new FileInputStream(file)))
    	            ;
        } catch (Exception e) {
        	e.printStackTrace();
            /*ArrayList l = new ArrayList();
            /*l.add(null);
            XMap map = new XMap(DataSource.class, l);
            IncidentInfo info = new IncidentInfo("ERROR DE PDT", null, null, e);
            map.put("ERROR_MESSAGE", info.getBasicErrorMessage());
            if (info.getErrorException() instanceof NullPointerException) {
                map.put("ERROR_MESSAGE", "Null pointer!");
            }
            String details = info.getDetailedErrorMessage();
            if (details == null) {
                if (info.getErrorException() != null) {
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    info.getErrorException().printStackTrace(pw);
                    details = sw.toString();
                } else {
                    details = "";
                }
            }
            map.put("ERROR", details);
            System.out.println("map=" + map);*/
            //return JR.open("/org/isobit/app/jr/error.jasper", map);
        	ss="ERROR";
        }
	    //InputStreamResource resource = new InputStreamResource(new FileInputStream(file));
        return null;
	}
	
    public static Object readObject(File file) throws IOException {
        FileInputStream fileIn = null;
        BufferedInputStream buffIn = null;
        ObjectInputStream obIn = null;

        try {
            fileIn = new FileInputStream(file);
            buffIn = new BufferedInputStream(fileIn);
            obIn = new ObjectInputStream(buffIn);
            return obIn.readObject();
        } catch (ClassNotFoundException cExc) {
            cExc.printStackTrace();
            return null;
        } finally {
            try {
                if (obIn != null) {
                    obIn.close();
                }
                if (buffIn != null) {
                    buffIn.close();
                }
                if (fileIn != null) {
                    fileIn.close();
                }
            } catch (IOException e) {
            }
        }
    }
	
}

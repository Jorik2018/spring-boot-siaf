package org.isobit.siaf.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.jamel.dbf.DbfReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestParam;

import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFRow;
import com.linuxense.javadbf.DBFUtils;

@Service
public class SiafServiceImpl implements SiafService {

	private Map<Integer, String> siafMap = new HashMap<Integer, String>();

	private Driver driver;

	@Autowired
	private TaskExecutor taskExecutor;

	private boolean bussy;
	
	@Value("${spring.datasource.username}")
	private String username;
	@Value("${spring.datasource.password}")
	private String pass;
	@Value("${spring.datasource.url}")
	private String url;
	@Value("${siaf.source}")
	private String source;
	@Value("${siaf.sec_ejec}")
	private Integer sec_ejec;
	@Value("${siaf.command}")
	private String command;

	private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public String escapeSpecialCharacters(Object dat) {
		if (dat instanceof java.util.Date)
			return simpleDateFormat.format((java.util.Date) dat);
		else if (dat instanceof Number)
			return dat.toString();
		else if (dat != null) {
			String data = dat.toString();
			String escapedData = data.replaceAll("\\R", " ");
			if (data.contains(",") || data.contains("\"") || data.contains("'")) {
				data = data.replace("\"", "\"\"");
				escapedData = "\"" + data + "\"";
			}
			return escapedData;
		} else
			return "";
	}

	public String convertToCSV(Object[] data) {
		return Stream.of(data).map(this::escapeSpecialCharacters).collect(Collectors.joining(","));
	}
	

	@Scheduled(fixedRate = 60 * 60 * 1000)
	public void syncSiaf() throws SQLException, FileNotFoundException {
		
		
		if(url.contains("oledb")) {
			if (bussy) {
				System.out.println("ocupado!");
				return;
			}
			bussy=true;
			if(command!=null)
				try {
					//String command="java -jar sync.jar \\\\Master2009\\Siaf-sp\\siaf_vfp\\Data";
					Process proc = Runtime.getRuntime().exec(command);
					InputStreamReader isr = new InputStreamReader(proc.getInputStream());
					BufferedReader rdr = new BufferedReader(isr);
					String line;
					while((line = rdr.readLine()) != null) { 
					  System.out.println(line);
					} 
	
					isr = new InputStreamReader(proc.getErrorStream());
					rdr = new BufferedReader(isr);
					while((line = rdr.readLine()) != null) { 
					  System.out.println(line);
					} 
					if (proc.waitFor() != 0) {
					    System.out.println("Error");
					} else {
					    System.out.println("Success!");
					}
				}catch(IOException | InterruptedException e) {e.printStackTrace();}
			else
				sync();
			bussy=false;
			return;
		}
		synkk(0);
	}
	
	public void synkk(int yea) throws SQLException {
		if(yea<=0) {
			Calendar calendar=Calendar.getInstance();
			yea=calendar.get(Calendar.YEAR);
		}
		if (bussy) {
			System.out.println("ocupado!");
			return;
		}
		bussy=true;
		Connection c = DriverManager.getConnection("jdbc:mysql://localhost/siaf?user="+username+"&password="+pass);
		/*
		 * Connection c2 = driver.connect(
		 * "jdbc:oledb:Provider=vfpoledb;Data Source=Y:\\\\siaf\\\\data;Collating Sequence=general"
		 * , null);
		 * 
		 */
		String folder=source;
		ResultSet rs = c.createStatement().executeQuery("SELECT @@GLOBAL.secure_file_priv");
		while (rs.next()) {
			folder = rs.getString(1);
			// folder=folder.replace("\\", File.separator);
			folder = folder.replace("\\", "/");
			System.out.println(folder);
		}
		
		long time = new java.util.Date().getTime();
		List<Object[]> generated=new ArrayList();
		for (String table : new String[] {
				"especifica_det",
				"expediente_documento", 
				"expediente_fase", 
				"expediente_secuencia",
				"expediente_meta",
				"expediente_nota",
				"finalidad",
				"fuente_financ",
				"meta", 
				"persona",		
				"maestro_clasificador"
			}) {
			
			File siafFile = new File(source, table + ".dbf");
			File csvOutputFile=null;
			if (siafFile.exists()) {
				String year=""+yea;
				
				time = new java.util.Date().getTime();
				DBFReader reader = null;
				try {
					
					csvOutputFile = new File(folder + table + "_"+year+".csv");
					if(csvOutputFile.exists()&&csvOutputFile.lastModified()>siafFile.lastModified())
						continue;
					reader = new DBFReader(new FileInputStream(siafFile));
					int numberOfFields = reader.getFieldCount();
					String ss = "", sset = "";
					boolean hasYear=false;
					for (int i = 0; i < numberOfFields; i++) { 
						DBFField field =reader.getField(i); 
						//System.out.println(field);
						if("ano_eje".equalsIgnoreCase(field.getName()))hasYear=true;					  
					}
					System.out.println("SYNC "+table);
					try (PrintWriter pw = new PrintWriter(
							new OutputStreamWriter(new FileOutputStream(csvOutputFile), "UTF-8"))) {
						Object[] row;
						int r = 0;
						//reader.skipRecords(r);
						if(hasYear)
						while ((row = reader.nextRecord(r++)) != null) {
							if (year.equals(row[0].toString())) {
								pw.println(convertToCSV(row));
							}
						}
						else
							while ((row = reader.nextRecord(r++)) != null) {
									pw.println(convertToCSV(row));
							}
						time = (new java.util.Date().getTime() - time) / 1000;
						System.out.println(table + ".csv created in " + (time / 60) + " min " + (time % 60) + " sec.");
						time = new java.util.Date().getTime() ;
					}
					ResultSet rst = c.createStatement()
							.executeQuery("SELECT * FROM " + table +  (hasYear?" WHERE ano_eje='"+year+"'":""));
					ResultSetMetaData rsmd = rst.getMetaData();
					List<String> names = new ArrayList();
					int columnCount = rsmd.getColumnCount();
					// The column count starts from 1
					for (int i = 1; i <= columnCount; i++) {
						names.add(rsmd.getColumnName(i));
					}
					for (int i = 0; i < numberOfFields; i++) {
						DBFField field = reader.getField(i);
						/// System.out.println(field.getName()+" "+field.getType());
						String name = names.get(i);
						if (field.getType().toString().equals("NUMERIC")) {
							ss += (",@" + field.getName());
							sset += ("," + name + " = (CASE WHEN @" + field.getName() + " REGEXP '^[0-9]+\\\\.?[0-9]*$' THEN @" + field.getName() + " ELSE null END)");
						} else if (field.getType().toString().equals("TIMESTAMP")
								|| field.getType().toString().equals("DATE")) {
							ss += (",@" + field.getName());
							sset += ("," + name + " = (CASE WHEN @" + field.getName()
									+ "='' THEN null ELSE STR_TO_DATE(@" + field.getName()
									+ ", '%Y-%m-%d %H:%i:%s') END)");
						} else {
							ss += ("," + name);
						}
					}
					if (ss.length() > 0)
						ss = "(" + ss.substring(1) + ") ";
					if (sset.length() > 0)
						ss += (" SET " + sset.substring(1));
					
					generated.add(new Object[]{csvOutputFile,
							"DELETE FROM " + table + (hasYear?" WHERE ano_eje='"+year+"'":""),
							"LOAD DATA INFILE '" + folder + csvOutputFile.getName() + "' " + "INTO TABLE "
							+ table + " " + "CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' " + "ENCLOSED BY '\"' "
							+ "LINES TERMINATED BY '\\n'\n" + ss});
				} catch (DBFException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (SQLException e) {
					if(csvOutputFile!=null)
						csvOutputFile.delete();
					e.printStackTrace();
				} finally {
					DBFUtils.close(reader);
				}
				/*
				 * 
				 * System.out.println("SYNC "+table); long time =new java.util.Date().getTime();
				 * ResultSet rs=c2.createStatement().executeQuery("SELECT * FROM "
				 * +table+" WHERE ano_eje='2021'"); ResultSetMetaData md = rs.getMetaData(); int
				 * nc = md.getColumnCount(); Object[] rowObjects; File csvOutputFile = new
				 * File("E:\\"+table+".csv"); try (PrintWriter pw = new
				 * PrintWriter(csvOutputFile)) { DBFRow row; while(rs.next()) { if(
				 * "2021".equals(rs.getString(1))) { Object[] d=new Object[nc]; for(int
				 * i=1;i<=nc;i++)d[i-1]=rs.getObject(i); pw.println(convertToCSV(d)); }
				 * //System.out.println(); } }
				 */
				/*
				 * List<Map> l = new ArrayList(); String col=""; for (int i = 0; i < nc; i++) {
				 * col+=(",?"); } String SQL =
				 * "INSERT INTO "+table+" VALUES("+(col.substring(1))+")";
				 * c.createStatement().executeUpdate("DELETE FROM "
				 * +table+" WHERE ano_eje='2021'"); PreparedStatement pstmt =
				 * c.prepareStatement(SQL); int n=0,cb=0; while(rs.next()) {
				 * pstmt.clearParameters(); for (int i = 1; i <= nc; i++) { pstmt.setObject(i,
				 * rs.getObject(i)); } pstmt.addBatch(); cb++;n++; if(cb==200) {
				 * pstmt.executeBatch(); System.out.println("executed "+cb+" of "+n); cb=0; } }
				 * if(cb>0) { pstmt.executeBatch(); System.out.println("executed "+cb+" of "+n);
				 * } pstmt.closeOnCompletion(); pstmt=null;
				 */
				time = (new java.util.Date().getTime() - time) / 1000;
			}
		}
		c.createStatement().executeUpdate("SET NAMES utf8mb4");
		for(Object[] file:generated) {
			try {
				System.out.println("importing "+file[0]);	
				time = new java.util.Date().getTime();
				c.createStatement().executeUpdate(""+file[1]);
				c.createStatement().executeUpdate(""+file[2]);
				time = (new java.util.Date().getTime() - time) / 1000;
				System.out.println("synchronized in " + (time / 60) + " min " + (time % 60) + " sec.");
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		c.close();
		// c2.close();
		System.out.println("disconet ");
		bussy = false;
	}

	@PostConstruct
	private void created() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		driver = (Driver) Class.forName("org.isobit.oledb.jdbc.OleDriver").newInstance();
		siafMap.put(1,
				"SELECT * FROM expediente_documento WHERE sec_ejec=:1 AND ano_eje=:2 AND expediente=:3 AND nombre like '%SARMIENTO TIRADO LINDOME%'");// fase='G'
		siafMap.put(2, ("SELECT " 
				+ "expediente_fase.ano_eje" 
				+ ",expediente_fase.expediente" 
				+ ",expediente_fase.ciclo"
				+ ",expediente_fase.fase" 
				+ ",expediente_fase.secuencia" 
				
				+ ",expediente_fase.ruc"
				+ ",expediente_fase.certificado" 
				+ ",expediente_fase.certificado_secuencia"
				+ ",expediente_secuencia.cod_doc" 
				+ ",expediente_secuencia.serie_doc"
				+ ",expediente_secuencia.num_doc" 
				+ ",expediente_secuencia.fecha_doc"
				
				+ ",expediente_secuencia.ano_cta_cte" 
				+ ",expediente_secuencia.banco" 
				+ ",expediente_secuencia.cta_cte" 

				+ ",expediente_secuencia.moneda" 
				+ ",sum(expediente_secuencia.monto) as monto"
				+ ",expediente_secuencia.estado_envio" 
				+ ",expediente_secuencia.mes_proceso"
				+ ",expediente_fase.fuente_financ" 
				+ ",fuente_financ.nombre AS fuente_financ_nombre"
				+ ",expediente_nota.secuencia_nota"

				+ ",expediente_nota.notas" 
				+ " FROM expediente_fase " 
				+ "INNER JOIN expediente_secuencia ON "
				+ "expediente_secuencia.ano_eje=expediente_fase.ano_eje "
				+ "AND expediente_secuencia.sec_ejec=expediente_fase.sec_ejec "
				+ "AND expediente_secuencia.expediente=expediente_fase.expediente "
				+ "AND expediente_secuencia.ciclo=expediente_fase.ciclo "
				+ "AND expediente_secuencia.fase=expediente_fase.fase "
				+ "AND expediente_secuencia.secuencia=expediente_fase.secuencia " 
				+ "LEFT OUTER JOIN expediente_nota "
				+ "ON expediente_fase.ano_eje=expediente_nota.ano_eje " 
				+ "AND expediente_fase.expediente=:expediente "
				+ "AND expediente_fase.sec_ejec=expediente_nota.sec_ejec "
				+ "AND expediente_fase.expediente=expediente_nota.expediente "
				+ "AND expediente_fase.ciclo=expediente_nota.ciclo " 
				+ "AND expediente_fase.fase=expediente_nota.fase "
				+ "AND expediente_fase.secuencia=expediente_nota.secuencia "
				+ "LEFT OUTER JOIN fuente_financ ON expediente_fase.ano_eje=fuente_financ.ano_eje "
				+ "AND expediente_fase.fuente_financ=fuente_financ.fuente_financ "
				+ "WHERE expediente_fase.ano_eje=:ano_eje " + "AND expediente_fase.sec_ejec=:sec_ejec "
				+ "AND expediente_fase.expediente=:expediente " 
				+ "GROUP BY expediente_fase.ano_eje"
				+ ",expediente_fase.expediente" 
				+ ",expediente_fase.ciclo" 
				+ ",expediente_fase.fase"
				+ ",expediente_fase.secuencia" 
				+ ",expediente_fase.ruc"
				+ ",expediente_fase.certificado"
				+ ",expediente_fase.certificado_secuencia" 
				+ ",expediente_secuencia.cod_doc"
				+ ",expediente_secuencia.serie_doc"
				+ ",expediente_secuencia.num_doc" 
				+ ",expediente_secuencia.fecha_doc" 
				+ ",expediente_secuencia.ano_cta_cte" 
				+ ",expediente_secuencia.banco" 
				+ ",expediente_secuencia.cta_cte" 
				+ ",expediente_secuencia.moneda"
				+ ",expediente_secuencia.estado_envio" + ",expediente_secuencia.mes_proceso" 
				+ ",expediente_fase.fuente_financ,fuente_financ.nombre"
				+ ",expediente_nota.secuencia_nota" + ",expediente_nota.notas " 
				+ " ORDER BY expediente_fase.ano_eje"
				+ ",expediente_fase.ciclo" 
				+ ",expediente_fase.fase" 
				+ ",expediente_fase.secuencia"
				+ ",expediente_secuencia.fecha_doc" 
				+ ",expediente_nota.secuencia_nota"));
		siafMap.put(3, "SELECT " 
				+ "expediente_meta.ciclo," 
				+ "expediente_meta.fase," 
				+ "expediente_meta.secuencia,"
				+ "expediente_meta.sec_func," 
				+ "expediente_meta.id_clasificador,"
				+ "especifica_det.descripcion AS especifica_det_descripcion,"
				+ "meta.nombre AS meta_nombre,"
				+ "meta.funcion," 
				+ "meta.programa,"
				+ "meta.sub_programa," 
				+ "meta.programa_ppto," 
				+ "meta.act_proy," 
				+ "meta.componente,"
				+ "meta.finalidad,"
				+ "finalidad.nombre AS finalidad_nombre,"
				+ "sum(expediente_meta.monto) AS monto," 
				+ "maestro_clasificador.clasificador "
				+ "FROM expediente_meta "
				+ "INNER JOIN meta ON meta.sec_ejec=expediente_meta.sec_ejec AND meta.ano_eje=expediente_meta.ano_eje AND meta.sec_func=expediente_meta.sec_func "
				+ "LEFT OUTER JOIN finalidad ON finalidad.ano_eje=meta.ano_eje AND finalidad.finalidad=meta.finalidad "
				+ "LEFT OUTER JOIN especifica_det ON especifica_det.ano_eje=expediente_meta.ano_eje AND especifica_det.id_clasificador=expediente_meta.id_clasificador "
				+ "INNER JOIN maestro_clasificador ON maestro_clasificador.ano_eje=expediente_meta.ano_eje AND maestro_clasificador.id_clasificador=expediente_meta.id_clasificador "
				+ "WHERE expediente_meta.sec_ejec=:sec_ejec AND expediente_meta.ano_eje=:ano_eje AND expediente_meta.expediente=:expediente "
				+ "GROUP BY expediente_meta.ciclo,expediente_meta.fase,"
				+ "expediente_meta.secuencia,expediente_meta.sec_func,"
				+ "meta.nombre,meta.funcion,meta.programa,meta.sub_programa,"
				+ "meta.programa_ppto,meta.act_proy,meta.componente,meta.finalidad,finalidad.nombre,"
				+ "expediente_meta.id_clasificador,especifica_det.descripcion,maestro_clasificador.clasificador ");
		siafMap.put(4, ("SELECT " + "expediente_secuencia.ano_eje" + ",expediente_secuencia.sec_ejec"
				+ ",expediente_secuencia.expediente" + ",expediente_secuencia.ciclo" + ",expediente_secuencia.fase"

				+ ",expediente_secuencia.secuencia" 
				+ ",expediente_secuencia.correlativo"
				+ ",expediente_secuencia.banco" 
				+ ",expediente_secuencia.cta_cte" + ",expediente_documento.cod_doc"

				+ ",expediente_documento.num_doc" 
				+ ",expediente_documento.nombre"
				+ ",expediente_secuencia.num_doc AS comprobante" 
				+ ",expediente_documento.fecha_doc AS fecha"
				+ ",expediente_documento.monto"

				+ ",expediente_documento.monto_nacional" 
				+ ",expediente_documento.estado"
				+ ",expediente_documento.estado_envio" 
				+ " FROM expediente_secuencia "
				+ "INNER JOIN expediente_documento ON expediente_documento.ano_eje=:2 "
				+ "AND expediente_documento.expediente=:3 "
				+ "AND expediente_secuencia.ano_eje=expediente_documento.ano_eje "
				+ "AND expediente_secuencia.sec_ejec=expediente_documento.sec_ejec "
				+ "AND expediente_secuencia.expediente=expediente_documento.expediente "
				+ "AND expediente_secuencia.ciclo=expediente_documento.ciclo "
				+ "AND expediente_secuencia.fase=expediente_documento.fase "
				+ "AND expediente_secuencia.secuencia=expediente_documento.secuencia "
				+ "AND expediente_secuencia.correlativo=expediente_documento.correlativo "
				+ " WHERE expediente_secuencia.ano_eje=:2 AND expediente_secuencia.expediente=:3 "
				+ " ORDER BY expediente_secuencia.ano_eje" + ",expediente_secuencia.ciclo"
				+ ",expediente_secuencia.fase" + ",expediente_secuencia.secuencia"
				+ ",expediente_secuencia.correlativo"));
		siafMap.put(6, "SELECT "
				+ "expediente_fase.ruc" 
				+ ",persona.nombre AS persona_nombre"
				+ ",expediente_fase.fase" 
				+ ",sum(expediente_fase.monto_nacional) AS monto "
				+ ",count(DISTINCT expediente_fase.expediente) AS count "
				+ " FROM expediente_fase "
				+ " INNER JOIN expediente_meta ON " 
				+ "expediente_meta.ano_eje=expediente_fase.ano_eje "
				+ "AND expediente_meta.sec_ejec=expediente_fase.sec_ejec "
				+ "AND expediente_meta.expediente=expediente_fase.expediente "
				+ "AND expediente_meta.ciclo=expediente_fase.ciclo " 
				+ "AND expediente_meta.fase=expediente_fase.fase "
				+ "AND expediente_meta.secuencia=expediente_fase.secuencia "
				+ "INNER JOIN persona ON persona.ruc=expediente_fase.ruc" + " WHERE "
				+ "expediente_fase.ano_eje=:ano_eje "
				+ "AND expediente_fase.sec_ejec=:sec_ejec "
				+ "GROUP BY "
				+ "expediente_fase.ruc" 
				+ ",persona.nombre"
				+ ",expediente_fase.fase"
				+ " ORDER BY expediente_fase.ruc"
		);
	}

	public ResultSet getResultSet(int id, int sec_eje, int ano_eje, Object delta) {
		System.out.println("delta=" + delta);
		String sql = (String) siafMap.get(id);
		try {
			sql = sql.replaceAll(":sec_ejec", "'" + String.format("%06d", sec_eje) + "'").replaceAll(":ano_eje",
					"'" + String.format("%04d", ano_eje) + "'");
			if (id == 6) {
				sql = sql.replaceAll(":ruc", "'" + delta + "'");
			} else
				sql = sql
						.replaceAll(":expediente",
								"'" + String.format("%010d", Integer.parseInt(delta.toString())) + "'")
						.replaceAll(":mes", "'" + String.format("%02d", Integer.parseInt(delta.toString())) + "'");
			System.out.println(sql);
			return c.createStatement().executeQuery(sql);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	static String toCamelCase(String s) {
		String[] parts = s.split("_");
		String camelCaseString = parts[0];
		for (int i = 1; i < parts.length; i++) {
			String part = parts[i];
			camelCaseString = camelCaseString + toProperCase(part);
		}
		return camelCaseString;
	}

	static String toProperCase(String s) {
		return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
	}

	private Connection c;

	@Override
	public Object getResultList(int report, int ano_eje, Object[] expediente) {
		return getResultList(report, ano_eje, expediente, Collections.EMPTY_MAP);
	}

	private class Meta{
		Map<Integer, String> label = new HashMap<Integer, String>();
		Map<String, Integer> columnIndex = new HashMap<String, Integer>();
		int nc;
		Meta(ResultSet rs){
			try {
				ResultSetMetaData md = rs.getMetaData();
				nc = md.getColumnCount();
				String name;
				for (int i = 1; i <= nc; i++) {
					name=toCamelCase(md.getColumnLabel(i));
					//System.out.println(md.getColumnName(i));
					label.put(i,name);
					columnIndex.put(name, i);
				}
			}catch(Exception ex) {
				throw new RuntimeException(ex);
			}
		}
	}
	
	@Override
	public Object getResultList(int report, int ano_eje, Object[] expediente, Map options) {
		ResultSet rs = null;
		int nc;
		try {
			HashMap<String, Object> m = null;
			Object result = null;
			ResultSetMetaData md;
			if (c == null) {
				if (url.contains("oledb"))
					c = driver.connect(url, null);
				else
					c = DriverManager.getConnection(url + "?user=root&password=" + pass);
			}
			Map<Integer, String> label = new HashMap<Integer, String>();
			Map<String, Integer> columnIndex = new HashMap<String, Integer>();
			List<Map> l = new ArrayList<Map>();
			if (report == 8) {
				l = getResultExpedienteAdministrativoList(c, ano_eje, sec_ejec,
						Integer.parseInt(expediente[0].toString()));
			} else if (report >= 4) {
				String keyGroup = "ruc";
				String keyGroup2 = "ruc";
				if (report == 6) {
					Map<String, String> meta = new HashMap<String, String>();
					meta.put("finalidad", "meta.finalidad");
					meta.put("finalidadNombre", "finalidad.nombre AS finalidad_nombre");
					meta.put("ruc", "expediente_fase.ruc");
					meta.put("metaNombre", "meta.nombre AS meta_nombre");
					meta.put("secFunc", "expediente_meta.sec_func");
					meta.put("proveedor", "persona.nombre AS persona_nombre");
					String[] select = options.get("select").toString().split(",");
					List<String> column = new ArrayList<String>();
					List<String> groupList = new ArrayList<String>();
					Map<String, String> selectMap = new HashMap<String, String>();
					for (String s : select)
						selectMap.put(s, meta.get(s));
					List<String> sortList = new ArrayList<String>();
					if ("finalidad".equals(select[0])) {
						column.add("expediente_meta.sec_func");
						groupList.add("expediente_meta.sec_func");
						column.add("meta.finalidad");
						groupList.add("meta.finalidad");
						column.add("meta.nombre AS metaNombre");
						groupList.add("meta.nombre");
						column.add("finalidad.nombre AS finalidad_nombre");
						groupList.add("finalidad.nombre");
						sortList.add("finalidad.nombre");
						keyGroup = "secFunc";
						keyGroup2 = "finalidad";
					} else {
						column.add("expediente_fase.ruc");
						groupList.add("expediente_fase.ruc");
						column.add("persona.nombre AS persona_nombre");
						groupList.add("persona.nombre");
						sortList.add("persona.nombre");
					}
					column.add("expediente_fase.fase");
					groupList.add("expediente_fase.fase");

					String sql = "SELECT " + String.join(",", column)
							+ ",sum(expediente_fase.monto_nacional) AS monto_fase "
							+ ",sum(expediente_meta.monto_nacional) AS monto "
							+ ",count(DISTINCT expediente_fase.expediente) AS count "

							+ " FROM expediente_fase "

							+ "INNER JOIN expediente_secuencia ON "
							+ "expediente_secuencia.ano_eje=expediente_fase.ano_eje "
							+ "AND expediente_secuencia.sec_ejec=expediente_fase.sec_ejec "
							+ "AND expediente_secuencia.expediente=expediente_fase.expediente "
							+ "AND expediente_secuencia.ciclo=expediente_fase.ciclo "
							+ "AND expediente_secuencia.fase=expediente_fase.fase "
							+ "AND expediente_secuencia.secuencia=expediente_fase.secuencia "

							+ "INNER JOIN expediente_meta ON " + "expediente_meta.ano_eje=expediente_fase.ano_eje "
							+ "AND expediente_meta.sec_ejec=expediente_fase.sec_ejec "
							+ "AND expediente_meta.expediente=expediente_fase.expediente "
							+ "AND expediente_meta.ciclo=expediente_fase.ciclo "
							+ "AND expediente_meta.fase=expediente_fase.fase "
							+ "AND expediente_meta.secuencia=expediente_fase.secuencia "

							+ (selectMap.containsKey("finalidad")
									? ("INNER JOIN meta ON " + "meta.ano_eje=expediente_meta.ano_eje "
											+ "AND meta.sec_ejec=expediente_meta.sec_ejec "
											+ "AND meta.sec_func=expediente_meta.sec_func " + "INNER JOIN finalidad ON "
											+ "finalidad.ano_eje=meta.ano_eje "
											+ "AND finalidad.finalidad=meta.finalidad ")
									: "")

							/*
							 * + "INNER JOIN maestro_clasificador ON " +
							 * "maestro_clasificador.ano_eje=expediente_meta.ano_eje " +
							 * "AND maestro_clasificador.id_clasificador=expediente_meta.id_clasificador "
							 */
							+ (selectMap.containsKey("proveedor")
									? "INNER JOIN persona ON persona.ruc=expediente_fase.ruc"
									: "")

							+ " WHERE " + "expediente_fase.ano_eje=:ano_eje "
							+ "AND expediente_fase.sec_ejec=:sec_ejec "
							// + "AND expediente_fase.ruc=:ruc "
							// + "AND expediente_secuencia.mes_proceso=:mes "
							// + "AND expediente_fase.fase='D' "
							// + "AND expediente_fase.fuente_financ='18' "
							+ "GROUP BY " + String.join(",", groupList) + " ORDER BY " + String.join(",", sortList);
					sql = sql.replaceAll(":sec_ejec", "'" + String.format("%06d", sec_ejec) + "'")
							.replaceAll(":ano_eje", "'" + String.format("%04d", ano_eje) + "'");
					System.out.println(sql);
					rs = c.createStatement().executeQuery(sql);
				} else if (report == 5) {
					result = this.getListReport5(c, sec_ejec, ano_eje, options);
					//System.out.println("result "+((List)result);
					
				} else {
					rs = getResultSet(report, sec_ejec, ano_eje, expediente[0]);
				}
				
				if(report!=5) {
					md = rs.getMetaData();
					nc = md.getColumnCount();
					for (int i = 1; i <= nc; i++) {
						// System.out.println(md.getColumnName(i));
						label.put(i, toCamelCase(md.getColumnLabel(i)));
						columnIndex.put(toCamelCase(md.getColumnLabel(i)), i);
					}
					int fase = columnIndex.get("fase");
					int monto = columnIndex.get("monto");
					if (report == 6) {
						String key = "", s;
						int group = columnIndex.get(keyGroup);
						int group2 = columnIndex.get(keyGroup2);
						while (rs.next()) {
							s = rs.getString(group) + '-' + rs.getString(group2);
							if (!s.equals(key)) {
								m = new HashMap<String, Object>();
								for (int i = 1; i <= nc; i++) {
									Object o = rs.getObject(i);
									if (o instanceof String)
										o = o.toString().trim();
									m.put(label.get(i), o);
								}
								BigDecimal bd = (BigDecimal) m.get(rs.getString(fase));
								BigDecimal bd2 = (BigDecimal) rs.getBigDecimal(monto);
								m.put(rs.getString(fase), bd != null ? bd.add(bd2) : bd2);
								l.add(m);
								key = s;
							} else {
								m.put(rs.getString(fase), rs.getBigDecimal(monto));
							}
						}
					} else {
						while (rs.next()) {
							m = new HashMap<String, Object>();
							for (int i = 1; i <= nc; i++) {
								Object o = rs.getObject(i);
								if (o instanceof String)
									o = o.toString().trim();
								m.put(label.get(15), o);
							}
							l.add(m);
						}
					}
					result = l;
				}
			} else {
				rs = getResultSet(2, sec_ejec, ano_eje, expediente[0]);
				md = rs.getMetaData();
				nc = md.getColumnCount();
				ArrayList<HashMap<String, Object>> l2 = new ArrayList<HashMap<String, Object>>();
				for (int i = 1; i <= nc; i++) {
					label.put(i, toCamelCase(md.getColumnLabel(i)));
					columnIndex.put(toCamelCase(md.getColumnLabel(i)), i);
				}
				String secuencia = "", s;
				int monto = columnIndex.get("monto");
				int notasIndex = columnIndex.get("notas");
				while (rs.next()) {
					s = rs.getString(4) + "-" + rs.getString(5);
					if (!s.equals(secuencia)) {
						m = new HashMap<String, Object>();
						for (int i = 1; i <= nc; i++) {
							if (i == monto) {
								m.put(label.get(i), rs.getBigDecimal(i));
							} else {
								Object o = rs.getObject(i);
								if (o instanceof String)
									o = o.toString().trim();
								m.put(label.get(i), o);
							}
						}
						l2.add(m);
						secuencia = s;
					} else {
						m.put("notas", m.get("notas") + rs.getString(notasIndex));
					}
				}
				rs = getResultSet(3, sec_ejec, ano_eje, expediente[0]);
				md = rs.getMetaData();
				nc = md.getColumnCount();
				label = new HashMap<Integer, String>();
				for (int i = 1; i <= nc; i++) {
					label.put(i, toCamelCase(md.getColumnLabel(i)));
					columnIndex.put(toCamelCase(md.getColumnLabel(i)), i);
				}
				HashMap<String, ArrayList<HashMap<String, Object>>> result2 = new HashMap<String, ArrayList<HashMap<String, Object>>>();
				result2.put("header", l2);
				l2 = new ArrayList<HashMap<String, Object>>();
				monto = columnIndex.get("monto");
				while (rs.next()) {
					m = new HashMap<String, Object>();
					for (int i = 1; i <= nc; i++) {
						if (i == monto) {
							m.put(label.get(i), rs.getBigDecimal(i));
						} else {
							Object o = rs.getObject(i);
							if (o instanceof String)
								o = o.toString().trim();
							m.put(label.get(i), o);
						}
					}
					l2.add(m);
				}
				result2.put("detail", l2);
				result = result2;
			}
			if (c != null) {
				c.close();
				System.out.println("SIAF closed");
				c = null;
			}
			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private List getResultExpedienteAdministrativoList(Connection cnx, int ano_eje,int sec_ejec, int expediente) throws Exception {
		List l=new ArrayList();
		Statement stmt=cnx.createStatement();
		String sql=("SELECT " 
				+ "expediente_fase.ano_eje" 
				+ ",expediente_fase.expediente" 
				+ ",expediente_fase.ciclo"
				+ ",expediente_fase.fase" 
				+ ",expediente_fase.secuencia" 
				
				+ ",expediente_fase.ruc"
				+ ",expediente_fase.certificado" 
				+ ",expediente_fase.certificado_secuencia"
				+ ",expediente_secuencia.cod_doc" 
				+ ",expediente_secuencia.num_doc" 
				+ ",expediente_secuencia.fecha_doc"
				+ ",expediente_secuencia.moneda" 
				+ ",sum(expediente_secuencia.monto) as monto"
				+ ",expediente_secuencia.estado_envio" 
				+ ",expediente_secuencia.mes_proceso" 
				+ ",fuente_financ.nombre AS fuente_financ"
				+ ",expediente_nota.secuencia_nota"
				
				+ ",expediente_nota.notas" 
				+ " FROM expediente_fase " 
				+ "INNER JOIN expediente_secuencia ON "
				+ "expediente_secuencia.ano_eje=expediente_fase.ano_eje "
				+ "AND expediente_secuencia.sec_ejec=expediente_fase.sec_ejec "
				+ "AND expediente_secuencia.expediente=expediente_fase.expediente "
				+ "AND expediente_secuencia.ciclo=expediente_fase.ciclo "
				+ "AND expediente_secuencia.fase=expediente_fase.fase "
				+ "AND expediente_secuencia.secuencia=expediente_fase.secuencia " 
				+ "LEFT OUTER JOIN expediente_nota "
				+ "ON expediente_fase.ano_eje=expediente_nota.ano_eje " 
				+ "AND expediente_fase.expediente=:expediente "
				+ "AND expediente_fase.sec_ejec=expediente_nota.sec_ejec "
				+ "AND expediente_fase.expediente=expediente_nota.expediente "
				+ "AND expediente_fase.ciclo=expediente_nota.ciclo " 
				+ "AND expediente_fase.fase=expediente_nota.fase "
				+ "AND expediente_fase.secuencia=expediente_nota.secuencia "
				+ "LEFT OUTER JOIN fuente_financ ON expediente_fase.ano_eje=fuente_financ.ano_eje "
				+ "AND expediente_fase.fuente_financ=fuente_financ.fuente_financ "
				+ "WHERE expediente_fase.ano_eje=:ano_eje " 
				+ "AND expediente_fase.sec_ejec=:sec_ejec "
				+ "AND expediente_fase.expediente=:expediente " 
				+ "GROUP BY expediente_fase.ano_eje"
				+ ",expediente_fase.expediente" 
				+ ",expediente_fase.ciclo" 
				+ ",expediente_fase.fase"
				+ ",expediente_fase.secuencia" 
				+ ",expediente_fase.ruc" 
				+ ",expediente_fase.certificado"
				+ ",expediente_fase.certificado_secuencia" 
				+ ",expediente_secuencia.cod_doc"
				+ ",expediente_secuencia.num_doc" 
				+ ",expediente_secuencia.fecha_doc" 
				+ ",expediente_secuencia.moneda"
				+ ",expediente_secuencia.estado_envio" 
				+ ",expediente_secuencia.mes_proceso" 
				+ ",fuente_financ.nombre"
				+ ",expediente_nota.secuencia_nota" 
				+ ",expediente_nota.notas " 
				+ " ORDER BY expediente_fase.ano_eje"
				+ ",expediente_fase.ciclo" 
				+ ",expediente_fase.fase" 
				+ ",expediente_fase.secuencia"
				+ ",expediente_secuencia.fecha_doc" 
				+ ",expediente_nota.secuencia_nota")
					.replace(":ano_eje", "'"+ano_eje+"'")
					.replace(":sec_ejec", "'"+String.format("%06d", sec_ejec)+"'")
					.replace(":expediente", "'"+String.format("%010d", expediente)+"'");
		System.out.println(sql);
		ResultSet rs=stmt.executeQuery(sql);
		ResultSetMetaData md = rs.getMetaData();
		int nc = md.getColumnCount();
		Map<Integer, String> label = new HashMap<Integer, String>();
		Map<String, Integer> columnIndex = new HashMap<String, Integer>();
		for (int i = 1; i <= nc; i++) {
			label.put(i, toCamelCase(md.getColumnLabel(i)));
			columnIndex.put(toCamelCase(md.getColumnLabel(i)), i);
		}
		int montoIndex = columnIndex.get("monto");
		int notasIndex = columnIndex.get("notas");
		String secuencia = "", s;
		HashMap m=null;
		while (rs.next()) {
			s=rs.getString(4)+"-"+rs.getString(5);
			if (!s.equals(secuencia)) {
				m=new HashMap();
				for (int i = 1; i <= nc; i++) {
					if (i == montoIndex) {
						m.put(label.get(i), rs.getBigDecimal(i));
					} else {
						Object o = rs.getObject(i);
						if (o instanceof String)
							o = o.toString().trim();
						m.put(label.get(i), o);
					}
				}
				l.add(m);
				secuencia = s;
			} else {
				m.put("notas", m.get("notas") + rs.getString(notasIndex));
			}
		}
		return l; 
	}

	public static String[] filesiaf = { 
			"especifica_det.dbf", 
			"especifica_det.cdx", 
			"expediente_documento.dbf",
			"expediente_documento.cdx",
			"expediente_fase.dbf", 
			"expediente_fase.cdx", 
			"expediente_meta.dbf", 
			"expediente_meta.cdx",
			"expediente_nota.dbf", 
			"expediente_nota.cdx",
			"expediente_secuencia.dbf", 
			"expediente_secuencia.cdx",
			"fuente_financ.dbf", 
			"fuente_financ.cdx",
			"meta.dbf", 
			"meta.cdx",
			"persona.dbf", 
			"persona.cdx",
			"maestro_clasificador.dbf",
			"maestro_clasificador.cdx", 
			"finalidad.dbf", 
			"finalidad.cdx", 
			"siaf.dbc", "siaf.dct", "siaf.dcx" };
	
	public static String DESTINY = "data";

	public void sync() {
		File folderSiaf = new File(source);
		File folderLocal = new File(DESTINY);
		if (!folderLocal.exists())
			folderLocal.mkdir();
		boolean force = false;
		double ziseTotal = 0.0D;
		for (String t : filesiaf) {
			File fSiaf = new File(folderSiaf, t);
			ziseTotal += fSiaf.length();
		}
		double zise = 0.0D;
		String format = "yyyy-MM-dd hh:mm:ss";
		for (String t : filesiaf) {
			File fSiaf = new File(folderSiaf, t);
			File fLocal = new File(folderLocal, t);
			try {
				if (!fLocal.exists())
					fLocal.createNewFile();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			zise += fSiaf.length();
			/*
			 * XDate.getLastWait(); X.log(" revisando actualizacion " + t);
			 * X.log("Local name=" + fLocal); X.log("Local lastModified=" +
			 * fLocal.lastModified()); X.log("Local lastModified=" + XDate.toString(new
			 * Date(fLocal.lastModified()), format)); X.log("Local size=" +
			 * fLocal.length()); X.log("Remote name=" + fSiaf); X.log("Remote lastModified="
			 * + fSiaf.lastModified()); X.log("Remote lastModified=" + XDate.toString(new
			 * Date(fSiaf.lastModified()), format)); X.log("Remote size=" + fSiaf.length());
			 */
			// notifyObservers("Actualizando " + fLocal.getName());
			if ((force) || fLocal.length() == 0 || (!fLocal.exists())
					|| (fLocal.lastModified() < fSiaf.lastModified())) {
				copyFile(fSiaf, fLocal);
				/*
				 * setChanged(); notifyObservers(Double.valueOf(zise / ziseTotal));
				 * setChanged(); notifyObservers(t + " actualizado");
				 */
			} else {
				// setChanged();
				// notifyObservers(t + " el archivo esta desde antes ");
			}
		}
	}

	public static void copyFile(File fileSource, File fileDestiny) {
		try {
			FileChannel in = new FileInputStream(fileSource).getChannel();
			FileChannel out = new FileOutputStream(fileDestiny).getChannel();
			System.out.println("Movidos " + in.transferTo(0L, fileSource.length(), out) + " bytes");
			in.close();
			out.close();
		} catch (Exception e) {
			System.out.println(e);
		}
	}
	
	private String println(String s) {
		System.out.println(s);
		return s;
	}
	
	public Object getListReport5(Connection cnx,Integer secEje,Integer anoEje,Map m) throws SQLException {
		Statement stmt=cnx.createStatement();
		String provider=(String) m.get("provider");
		String ruc=(String) m.get("ruc");
		String purpose=(String) m.get("purpose");
		Integer from=(Integer) m.get("from");
		Integer to=(Integer) m.get("to");
		Integer month=(Integer) m.get("month");
		Integer secFunc=(Integer) m.get("secFunc");
		
		//Esto solo corre con Mysql
		//SELECT FOUND_ROWS();
		ResultSet rs=stmt.executeQuery(println("SELECT SQL_CALC_FOUND_ROWS "
				+ "t.sec_func"
				+ ",t.clasificador"
				+ ",t.fuente_financ"
				+ ",t.expediente"
				+ ",t.ruc"
				+ ",t.persona_nombre"
				+ ",t.id_clasificador"
				+ ",t.act_proy" 
				+ ",t.componente"
				+ ",t.funcion" 
				+ ",t.programa" 
				+ ",t.sub_programa " 
				+ ",t.meta " 
				+ ",t.finalidad"
				+ ",t.finalidad_nombre"
				+ ",MAX(t.count) " 
				+ ",SUM(CASE WHEN fase='C' THEN monto ELSE 0 END) AS C"
				+ ",SUM(CASE WHEN fase='D' THEN monto ELSE 0 END) AS D"
				+ ",SUM(CASE WHEN fase='G' THEN monto ELSE 0 END) AS G"
				+ ",SUM(CASE WHEN fase='P' THEN monto ELSE 0 END) AS P"
				+ " FROM ("
				+ "SELECT expediente_meta.sec_func" 
				+ ",maestro_clasificador.clasificador"
				+ ",expediente_fase.expediente" 
				+ ",expediente_fase.fuente_financ,"
				+ "expediente_fase.ruc" 
				+ ",persona.nombre AS persona_nombre"
				+ ",expediente_meta.id_clasificador" 
				
				+ ",meta.act_proy" 
				+ ",meta.componente" 
				+ ",meta.funcion"
				+ ",meta.programa"
				+ ",meta.sub_programa " 
				+ ",meta.meta " 
				+ ",meta.finalidad" 
				+ ",finalidad.nombre AS finalidad_nombre"
				
				+ ",expediente_fase.fase"
				+ ",count(expediente_fase.expediente) AS count " 
				+ ",sum(expediente_fase.monto_nacional) AS monto "
				/*+ ",sum(expediente_meta.monto_nacional) AS monto_meta " */
				+ " FROM expediente_fase "
				+ "INNER JOIN expediente_secuencia ON " 
				+ "expediente_secuencia.ano_eje=expediente_fase.ano_eje "
				+ "AND expediente_secuencia.sec_ejec=expediente_fase.sec_ejec "
				+ "AND expediente_secuencia.expediente=expediente_fase.expediente "
				+ "AND expediente_secuencia.ciclo=expediente_fase.ciclo "
				+ "AND expediente_secuencia.fase=expediente_fase.fase "
				+ "AND expediente_secuencia.secuencia=expediente_fase.secuencia " 
				+ "INNER JOIN expediente_meta ON "
				+ "expediente_meta.ano_eje=expediente_fase.ano_eje "
				+ "AND expediente_meta.expediente=expediente_fase.expediente "
				+ "AND expediente_meta.ciclo=expediente_fase.ciclo " 
				+ "AND expediente_meta.fase=expediente_fase.fase "
				+ "AND expediente_meta.secuencia=expediente_fase.secuencia " 
				+ "INNER JOIN meta ON "
				+ "meta.ano_eje=expediente_meta.ano_eje " 
				+ "AND meta.sec_ejec=expediente_meta.sec_ejec "
				+ "AND meta.sec_func=expediente_meta.sec_func " 
				+ "INNER JOIN finalidad ON "
				+ "finalidad.ano_eje=meta.ano_eje " 
				+ "AND finalidad.finalidad=meta.finalidad "
				+ "INNER JOIN maestro_clasificador ON " 
				+ "maestro_clasificador.ano_eje=expediente_meta.ano_eje "
				+ "AND maestro_clasificador.id_clasificador=expediente_meta.id_clasificador "
				+ "INNER JOIN persona ON persona.ruc=expediente_fase.ruc" 
				+ " WHERE "
				+ "expediente_fase.ano_eje='" + String.format("%04d", anoEje) + "' "
				+ "AND expediente_fase.sec_ejec='" + String.format("%06d", secEje) + "' "
				+ (secFunc!=null?("AND expediente_meta.sec_func='"+(String.format("%04d", secFunc))+"'"):"")
				+ (month!=null?("AND expediente_secuencia.mes_proceso='"+(String.format("%02d", month))+"'"):"")
				+ (provider!=null?("AND UPPER(persona.nombre) LIKE '%"+provider.toUpperCase()+"%'"):"")
				+ (ruc!=null?("AND expediente_fase.ruc LIKE '%"+ruc+"%'"):"")
				+ (purpose!=null?("AND UPPER(finalidad.nombre) LIKE '%"+purpose.toUpperCase()+"%'"):"")
				// + "AND expediente_fase.fase='D' "
				/*+ "AND expediente_fase.fuente_financ='18' " */
				+ "GROUP BY "
				+ "expediente_meta.sec_func"
				+ ",maestro_clasificador.clasificador" 
				+ ",expediente_fase.expediente" 
				+ ",expediente_fase.ruc"
				+ ",persona.nombre" 
				+ ",expediente_meta.id_clasificador" 
				+ ",meta.act_proy" 
				+ ",meta.componente"
				+ ",meta.funcion" 
				+ ",meta.programa" 
				+ ",meta.sub_programa " 
				+ ",meta.meta " 
				+ ",meta.finalidad"
				+ ",finalidad.nombre" 
				+ ",expediente_fase.fase"
				/*+ " ORDER BY expediente_meta.sec_func" 
				+ ",maestro_clasificador.clasificador"
				+ ",expediente_fase.expediente"*/
				
				+ ") t GROUP BY "
				+ "t.sec_func"
				+ ",t.clasificador" 
				+ ",t.fuente_financ" 
				+ ",t.expediente" 
				+ ",t.ruc"
				+ ",t.persona_nombre" 
				+ ",t.id_clasificador" 
				+ ",t.act_proy" 
				+ ",t.componente"
				+ ",t.funcion" 
				+ ",t.programa" 
				+ ",t.sub_programa " 
				+ ",t.meta " 
				+ ",t.finalidad"
				+ ",t.finalidad_nombre" 
				+ " ORDER BY t.sec_func" 
				+ ",t.clasificador"
				+ ",t.expediente"
				+ (from!=null?" LIMIT "+from+","+to:"")
				)
				);
				ArrayList l = new ArrayList();
				Meta meta = new Meta(rs);
				HashMap row;
				// Esto es para poner los montos de fase en columnas
				while (rs.next()) {
					row = new HashMap<String, Object>();
					for (int i = 1; i <= meta.nc; i++) {
						Object o = rs.getObject(i);
						if (o instanceof String)
							o = o.toString().trim();
						if (o instanceof Number && ((Number) o).doubleValue() == 0)
							o = null;
						row.put(meta.label.get(i), o);
					}
					l.add(row);
				}
				m.put("data", l);
				System.out.println("SSSSize="+l.size());
				rs = stmt.executeQuery("SELECT FOUND_ROWS()");
				if (rs.next())
					m.put("size", rs.getInt(1));
				return m;
	}

}

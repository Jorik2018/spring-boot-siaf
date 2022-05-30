package org.isobit.siaf.service;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface SiafService {

	public Object getResultList(int report,int ano_eje,Object[] expediente);
	
	public Object getResultList(int report,int ano_eje,Object[] expediente,Map m);
	
	public void sync();
	
	public void syncSiaf() throws SQLException, FileNotFoundException;
	
	public void synkk(int yea) throws SQLException;
	
}

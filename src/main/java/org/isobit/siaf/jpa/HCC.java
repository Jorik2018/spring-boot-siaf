package org.isobit.siaf.jpa;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;


@Entity
@Table(name = "hcc")
public class HCC implements Serializable {
    private static final long serialVersionUID = 1L;
    @Id
    @Basic(optional = false)
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;
    
    @Column(name="year")
    private Integer anoEje;

    private Integer month;
    
    @Transient
    private Object ext;
    
    private Integer number;
    
    private BigDecimal monto;
    
    private Integer expediente;
    
    private Character fase;
    
    private Integer secuencia;
    
    @Column(name="sec_func")
    private Integer secFunc;
    
    @Temporal(TemporalType.TIMESTAMP)
    private Date creation;

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof HCC)) {
            return false;
        }
        HCC other = (HCC) object;
        if ((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id))) {
            return false;
        }
        return true;
    }
    
    public Integer getId() {
		return id;
	}

	public Integer getSecFunc() {
		return secFunc;
	}

	public void setSecFunc(Integer secFunc) {
		this.secFunc = secFunc;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Integer getAnoEje() {
		return anoEje;
	}

	public void setAnoEje(Integer anoEje) {
		this.anoEje = anoEje;
	}
	
	public Integer getMonth() {
		return month;
	}

	public void setMonth(Integer month) {
		this.month = month;
	}

	public Character getFase() {
		return fase;
	}

	public void setFase(Character fase) {
		this.fase = fase;
	}

	public Integer getNumber() {
		return number;
	}

	public void setNumber(Integer number) {
		this.number = number;
	}

	public BigDecimal getMonto() {
		return monto;
	}

	public void setMonto(BigDecimal monto) {
		this.monto = monto;
	}

	public Integer getSecuencia() {
		return secuencia;
	}

	public void setSecuencia(Integer secuencia) {
		this.secuencia = secuencia;
	}

	public Integer getExpediente() {
		return expediente;
	}

	public Date getCreation() {
		return creation;
	}

	public void setCreation(Date creation) {
		this.creation = creation;
	}

	public Object getExt() {
		return ext;
	}

	public void setExt(Object ext) {
		this.ext = ext;
	}

	public void setExpediente(Integer expediente) {
		this.expediente = expediente;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	@Override
    public String toString() {
        return "org.isobit.siaf.jpa.HCC[ id=" + id + ", year="+this.anoEje+", month="+this.month+" ]";
    }
    
}

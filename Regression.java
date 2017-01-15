import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


/**
 * Cette classe est produite par les Mappers à destination du Reducer
 * Elle représente un triplet des sommes partielles (n, Σx, Σx²) permettant
 * de calculer la variance de n valeurs x
 */
public class Regression implements Writable
{
    /* variables membre */
    private long n;
    private double Sx;
    private double Sx2;
    
    private double Sy;
    private double Sy2;
    
    private double SumXxY;

    /** constructeur */
    public Regression()
    {
        n = 0L;
        Sx = 0.0;
        Sx2 = 0.0;
        
        Sy = 0.0;
        Sy2 = 0.0;
        
        SumXxY = 0.0;
    }

    /** écrit this sur sortie, méthode de l'interface Writable */
    public void write(DataOutput sortie) throws IOException
    {
        sortie.writeLong(n);
        sortie.writeDouble(Sx);
        sortie.writeDouble(Sx2);
        
        sortie.writeDouble(Sy);
        sortie.writeDouble(Sy2);
        
        sortie.writeDouble(SumXxY);
    }

    /** lit this à partir de l'entree, méthode de l'interface Writable */
    public void readFields(DataInput entree) throws IOException
    {
        n = entree.readLong();
        Sx = entree.readDouble();
        Sx2 = entree.readDouble();
        
        Sy = entree.readDouble();
        Sy2 = entree.readDouble();
        
        SumXxY = entree.readDouble();
    }

    /**
     * initialise this à (1, valeur, valeur²)
     * @param valeur à mettre dans this
     */
    public void set(double x, double y)
    {
        n = 1L;
        Sx = x;
        Sx2 = x*x;
        
        Sy = y;
        Sy2 = y*y;
        
        SumXxY = x*y;
    }
    

    /**
     * ajoute autre à this
     * @param other
     */
    public void add(Regression other)
    {
        n += other.n;
        Sx += other.Sx;
        Sx2 += other.Sx2;
        
        Sy += other.Sy;
        Sy2 += other.Sy2;
        
        SumXxY += other.SumXxY;
    }
    
    public double getSx(){
    	return Sx;
    }
    
    public double getSx2(){
    	return Sx2;
    }
    
    public double getSy(){
    	return Sy;
    }
    
    public double getSy2(){
    	return Sy2;
    }
    
    public double getSumXxY(){
    	return SumXxY;
    }
    

    /**
     * calcule la moyenne représentée par this
     * @return moyenne des valeurs ajoutées à this
     */
    public double getXAverage()
    {
        return Sx/n;
    }
    
    public double getX2Average()
    {
        return Sx2/n;
    }
    
    public double getYAverage()
    {
        return Sy/n;
    }
    
    public double getY2Average()
    {
        return Sy2/n;
    }

    /**
     * calcule la variance représentée par this
     * @return variance des valeurs ajoutées à this
     */
    public double getXVariance()
    {
        double Mx = Sx / n;
        double Mx2 = Sx2 / n;
        return Mx2 - Mx * Mx;
    }
    
    public double getYVariance()
    {
        double Mx = Sy / n;
        double Mx2 = Sy2 / n;
        return Mx2 - Mx * Mx;
    }
    
    public double getCovariance()
    {
    	return SumXxY / n - getXAverage() * getYAverage();
    }
    
    public double getCorrelation(){
    	double denom = Math.sqrt(getXVariance() * getYVariance());
    	double covariance = getCovariance();
    	if (covariance < 1 && covariance > -1 && denom < 1 && denom > -1){
    		return 1;
    	}
    	return covariance/denom;
    }
    
    public double[] getLinearModel(){
    	double[] linearModel = new double[2];
    	linearModel[1] = getCovariance() / getXVariance();
    	linearModel[0] = getYAverage() - linearModel[1] * getXAverage();
    	return linearModel;
    }
    
    public long getN(){
    	return n;
    }
    
    
}

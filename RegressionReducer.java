import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Cette classe effectue la réduction de toutes les paires ("V", (n, Σx, Σx²)),
 * calcule la variance et émet une paire ("V", Vx)
 */
public class RegressionReducer
        extends Reducer<Text, Regression, Text, DoubleWritable>
{
    /* valeur de sortie, la clé est la même qu'en entrée */
    private DoubleWritable valeurS = new DoubleWritable();

    /** traite une liste de paires produites par tous les mappers et combiners */
    @Override
    public void reduce(Text cleI, Iterable<Regression> valeursI, Context context)
            throws IOException, InterruptedException
    {
        
        // calculer la valeur de sortie
        Regression regression = new Regression();
        for (Regression valeurI : valeursI) {
            regression.add(valeurI);
            
        }
        
        // 1a
        if(regression.getN() < 2){
        	valeurS.set(0.0);
        	context.write(new Text("n < 2"), valeurS);
        	return;
        }
        
        valeurS.set(regression.getN());
        context.write(new Text("n"), valeurS);
        
        // 1b
        valeurS.set(regression.getSx());
        context.write(new Text("Sx"), valeurS);
        
        // 1c
        valeurS.set(regression.getSx2());
        context.write(new Text("Sx²"), valeurS);
        
        // Variance
        valeurS.set(regression.getXVariance());
        context.write(new Text("X Variance"), valeurS);
        
        // 1d
        valeurS.set(regression.getSy());
        context.write(new Text("Sy"), valeurS);
        
        // 	1e
        valeurS.set(regression.getSy2());
        context.write(new Text("Sy²"), valeurS);
        
        // 1f 
        valeurS.set(regression.getSumXxY());
        context.write(new Text("Sum X x Y"), valeurS);
        
        // 2a
        valeurS.set(regression.getXAverage());
        context.write(new Text("X average"), valeurS);
        
        // 2b
        valeurS.set(regression.getX2Average());
        context.write(new Text("X² average"), valeurS);
        
        // 	2c
        valeurS.set(regression.getYAverage());
        context.write(new Text("Y average"), valeurS);
        
        // 	2d
        valeurS.set(regression.getY2Average());
        context.write(new Text("Y² average"), valeurS);
        
        // 2e
        valeurS.set(regression.getSumXxY() / regression.getN());
        context.write(new Text("X x Y average"), valeurS);
        
        // 3a
        valeurS.set(regression.getXVariance());
        context.write(new Text("X variance"), valeurS);
        
        // 3b
        valeurS.set(regression.getYVariance());
        context.write(new Text("Y variance"), valeurS);
        
        // 3c
        valeurS.set(regression.getCovariance());
        context.write(new Text("Covariance"), valeurS);
        
        // 3d
        valeurS.set(regression.getCorrelation());
        context.write(new Text("Corrélation r"), valeurS);
        
        // 3e & 3f
        double[] linearModel = regression.getLinearModel();
        valeurS.set(linearModel[0]);
        context.write(new Text("β0"), valeurS);
        valeurS.set(linearModel[1]);
        context.write(new Text("β1"), valeurS);
        valeurS.set(0d);
        context.write(new Text("yi = "+linearModel[0]+" + "+linearModel[1]+" * xi + ei"), valeurS);
        
    }
}

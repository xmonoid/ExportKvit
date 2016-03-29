/**
 * 
 */
package ru.nlmk_it.db2file.args;

import java.io.File;
import java.io.PrintStream;

import org.apache.log4j.Logger;

import ru.nlmk_it.db2file.database.SQLScript;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

/**
 * The set of program parameters.
 * @author kosyh_ev
 * @version 0.1
 */
public final class Arguments {
	
	private static final Logger logger = Logger.getLogger(Arguments.class);

    @Parameter(names="-help",
            description="Print this help and exit",
            help=true)
    private boolean help;
    
    @Parameter(names="-notitles",
    		description="Export file without titles of fields (not all file types)",
    		required=false)
    private boolean withoutTitles;
    
    @Parameter(names={"-filetype", "-type"},
            description="The type of the export file",
            required=false,
            converter=FiletypeEnumConverter.class)
    private FiletypeEnum filetype;
    
    @Parameter(names="-query",
            description="The file with SQL query",
            required=true,
            converter=SQLScriptConverter.class)
    private SQLScript sqlScript;
    
    @Parameter(names="-Pleskgesk",
    		   description="The name of company (e.g. LESK, GESK)",
    		   required=true)
    private String pleskgesk;
    
    @Parameter(names="-Pdat",
 		   description="The date on which it will download",
 		   required=true)
    private String pdat;
    
    @Parameter(names="-Pdb_lesk",
 		   description="The code of database for required region",
 		   required=true)
    private String pdb_lesk;
    
    @Parameter(names="-Pnot_empty",
  		   description="Set this to 1 if you want to exclude empty bills or 0 otherwise",
  		   required=true)
    private String pnot_empty;
    
    @Parameter(names="-Puse_filter",
    		description="Set this to 1 if you want to use filter table or 0 otherwise",
    		required=false)
    private String use_filter = "0";
    
    @Parameter(names="-no_procedure",
    		description="Don't use stored procedure which creates data",
    		required=false)
    private boolean noProcedure;
    
    @Parameter(names="-export_dir",
			description="Каталог, в который экспортируются файлы",
			required=false,
			converter=ExportDirConverter.class)
	private File exportDir = new File("./export/");
    
    @Parameter(names="-Pmkd_id",
    		description="ID отдельного МКД",
    		required=false)
    private String mkd_id = "-1";
    
    /**
     * The class constructor.
     */
    public Arguments() {
    	logger.trace("Created an object: " + this.toString());
    }

    public boolean isWithoutTitles() {
    	return this.withoutTitles;
    }

    public void setFiletype(String arg0) {
    	this.filetype = FiletypeEnum.fromString(arg0);
    }
    
    public String getFilename() {
    	StringBuilder result = new StringBuilder("Ab");
    	
    	if (sqlScript.getScriptName().indexOf("notmkd") < 0) {
    		result = result.append("_mkd");
    	}
    	
    	return result.toString();
    }

    public FiletypeEnum getFiletype() {
        return filetype;
    }

    public SQLScript getQueries() {
        return sqlScript;
    }
    
    public String getPleskgesk() {
    	return pleskgesk;
    }
    
    public String getPdb_lesk() {
    	return pdb_lesk;
    }
    
    public String getPnot_empty() {
    	return pnot_empty;
    }
    
    public String getPdat() {
    	return pdat;
    }
    
    public String getUseFilter() {
    	return use_filter;
    }
    
    public String getMkdId() {
    	return mkd_id;
    }
    
    public boolean isNoProcedure() {
    	return noProcedure;
    }
    
    public boolean isHelp() {
        return help;
    }
    
    /**
	 * Каталог, в который должны сохраняться создаваемые файлы.
	 * @return
	 */
	public File getExportDir() {
		logger.trace("Invoke getExportDir()");
		logger.trace("Return value -> " + exportDir.getAbsolutePath());
		return exportDir;
	}
    
    /**
     * Print fields of this object.
     * @param out The instance of class OutputStream.
     */
    public void print(PrintStream out) {
    	out.println("notitles  = " + this.withoutTitles);
    	out.println("filetype  = " + this.filetype);
    	out.println("query     = " + this.sqlScript);
    	out.println("Pleskgesk = " + this.pleskgesk);
    	out.println("Pdb_lesk  = " + this.pdb_lesk);
    	out.println("Pnot_empty= " + this.pnot_empty);
    	out.println("Pdat      = " + this.pdat);
    	out.println("use_filter   = " + this.use_filter);
    	out.println("no_procedure = " + this.noProcedure);
    	out.println("mkd_id    = " + this.mkd_id);
    }

    
    /**
     * Validate set parameters. If any parameter is incorrect then throw an instance of {@link ParameterException} class.
     */
	public void validate() {
		logger.trace("Invoke validate()");
		if (filetype == null) {
			throw new ParameterException("The following option is required: -filetype, -type");
		}
		if (sqlScript == null) {
			throw new ParameterException("The following option is required: -query");
		}
	}
}

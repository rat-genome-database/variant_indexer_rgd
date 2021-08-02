package edu.mcw.rgd.variantIndexerRgd.dao;

import org.springframework.jdbc.object.MappingSqlQuery;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

public class VariantIndexQuery extends MappingSqlQuery {
   public VariantIndexQuery(DataSource ds, String query){
        super(ds, query);
    }
    @Override
    protected Object mapRow(ResultSet rs, int rowNum) throws SQLException {
        return null;
    }
}

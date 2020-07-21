package com.pth;

import com.pth.domain.ReportTesting;
import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.engine.JasperExportManager;
import net.sf.jasperreports.engine.JasperFillManager;
import net.sf.jasperreports.engine.JasperPrint;
import net.sf.jasperreports.engine.data.JRBeanCollectionDataSource;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

public class JasperReportProcessing {

    private static final String JASPER_FILE_PATH = "reports/testing.jasper";

    private JasperReportProcessing() {}

    public static void exportPdf(ReportTesting data, final String suffix) {
        try {
            File jasperFile =
                    new File(JasperReportProcessing.class.getClassLoader().getResource(JASPER_FILE_PATH).getFile());

            JasperPrint jasperPrint = JasperFillManager.fillReport(new FileInputStream(jasperFile), null, new JRBeanCollectionDataSource(Arrays.asList(data)));

            /*JasperExportManager.exportReportToPdfFile(jasperPrint,
                    String.format("C:\\reports\\out\\outFile_%s%s", suffix, ".pdf"));*/
            JasperExportManager.exportReportToPdfFile(jasperPrint,
                    String.format("D:\\Work\\IntelliJ\\mypc\\src\\main\\resources\\reports\\out\\out_%s%s", suffix, ".pdf"));
        } catch (IOException ex) {
            System.out.println(ex);
        } catch (JRException jrEx) {
            System.out.println(jrEx);
        }
    }

}

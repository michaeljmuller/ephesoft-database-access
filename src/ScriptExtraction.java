import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.output.XMLOutputter;
import org.jdom.xpath.XPath;

import com.ephesoft.dcma.script.IJDomScript;
import com.ephesoft.dcma.util.FileUtils;
import com.ephesoft.dcma.util.XMLUtil;

/**
 * The <code>ScriptExtraction</code> class represents the ScriptExtraction structure. Writer of scripts plug-in should implement this
 * IScript interface to execute it from the scripting plug-in. Via implementing this interface writer can change its java file at run
 * time. Before the actual call of the java Scripting plug-in will compile the java and run the new class file.
 * 
 * @author Ephesoft
 * @version 1.0
 */
public class ScriptExtraction implements IJDomScript {

	private static final String BATCH_LOCAL_PATH = "BatchLocalPath";
	private static final String BATCH_INSTANCE_ID = "BatchInstanceIdentifier";
	private static final String EXT_BATCH_XML_FILE = "_batch.xml";
	private static String ZIP_FILE_EXT = ".zip";

	public Object execute(Document documentFile, String methodName, String docIdentifier) {
		Exception exception = null;
		try {
			System.out.println("*************  Inside ScriptExtraction scripts.");
			System.out.println("*************  Start execution of the ScriptExtraction scripts.");

			if (null == documentFile) {
				System.out.println("Input document is null.");
			}

			// get the batch ID
			String batchId = documentFile.getRootElement().getChildText("BatchInstanceIdentifier");
			
			// get the database connection properties from the ephesoft configuration
    		Properties p = new Properties();
			String home = System.getenv("DCMA_HOME");
			File propFile = new File(home, "WEB-INF/classes/META-INF/dcma-data-access/dcma-db.properties");
			InputStream is = new FileInputStream(propFile);
			p.load(is);
    		
			// get the connection information from the properties file
			String username = (String) p.get("dataSource.username");
			String password = (String) p.get("dataSource.password");
			String driver = (String) p.get("dataSource.driverClassName");
			String db = (String) p.get("dataSource.databaseName");
			String server = (String) p.get("dataSource.serverName");
			String url = (String) p.get("dataSource.url");

			// fix the URL by substituting in the parameters
			url = url.replace("${dataSource.serverName}", server);
			url = url.replace("${dataSource.databaseName}", db);
			url = url.replace("${dataSource.username}", username);
			url = url.replace("${dataSource.password}", password);
			
			// get a connection to the database
			Class.forName(driver).newInstance();
			Connection c = DriverManager.getConnection(url, username, password);
			
			// get the time this batch started
			String sql = "select creation_date from batch_instance where identifier = ?";
			PreparedStatement statement = c.prepareStatement(sql);
			statement.setString(1, batchId);
			ResultSet rs = statement.executeQuery();
			rs.next();
			Date batchStartDate = rs.getDate(1);
				
			// find all the scan date document-level fields
			String xpath = "/Batch/Documents/Document/DocumentLevelFields/DocumentLevelField[Name='ScanDate']";
			@SuppressWarnings("unchecked")
			List<Element> fields = XPath.selectNodes(documentFile, xpath);
			for (Element field : fields) {
				
				// get the value element (create it if it doesn't exist)
				Element value = field.getChild("Value");
				if (value == null) {
					value = new Element("Value");
					field.addContent(value);
				}
				
				// set the value 
				value.setText(batchStartDate.toString());
			}

			boolean isWrite = true;
			// write the document object to the XML file.
			if (isWrite) {
				writeToXML(documentFile);
				System.out.println("*************  Successfully write the xml file for the ScriptExtraction scripts.");
				System.out.println("*************  End execution of the ScriptExtraction scripts.");
			}
		} catch (Exception e) {
			System.out.println("*************  Error occurred in scripts." + e.getMessage());
			exception = e;
		}
		return exception;
	}


	public static void main(String args[]){
		String zipFilePath = "C:\\Users\\Administrator\\workspace\\EphesoftDatabase\\testData\\BI1E_batch.xml";
		String xmlFilename = zipFilePath.substring(zipFilePath.lastIndexOf('\\')+1);
		try {
			Document doc = XMLUtil.createJDOMDocumentFromInputStream(FileUtils.getInputStreamFromZip(zipFilePath, xmlFilename));
			ScriptExtraction  se = new ScriptExtraction();
			se.execute(doc, null, null);
		} catch (Exception x) {
			x.printStackTrace();
		}
	}	
	/**
	 * The <code>writeToXML</code> method will write the state document to the XML file.
	 * 
	 * @param document {@link Document}.
	 */
	private void writeToXML(Document document) {
		String batchLocalPath = null;
		List<?> batchLocalPathList = document.getRootElement().getChildren(BATCH_LOCAL_PATH);
		if (null != batchLocalPathList) {
			batchLocalPath = ((Element) batchLocalPathList.get(0)).getText();
		}

		if (null == batchLocalPath) {
			System.err.println("Unable to find the local folder path in batch xml file.");
			return;
		}

		String batchInstanceID = null;
		List<?> batchInstanceIDList = document.getRootElement().getChildren(BATCH_INSTANCE_ID);
		if (null != batchInstanceIDList) {
			batchInstanceID = ((Element) batchInstanceIDList.get(0)).getText();

		}

		if (null == batchInstanceID) {
			System.err.println("Unable to find the batch instance ID in batch xml file.");
			return;
		}

		String batchXMLPath = batchLocalPath.trim() + File.separator + batchInstanceID + File.separator + batchInstanceID
				+ EXT_BATCH_XML_FILE;

		String batchXMLZipPath = batchXMLPath + ZIP_FILE_EXT;

		System.out.println("batchXMLZipPath************" + batchXMLZipPath);

		OutputStream outputStream = null;
		File zipFile = new File(batchXMLZipPath);
		FileWriter writer = null;
		XMLOutputter out = new XMLOutputter();
		try {
			if (zipFile.exists()) {
				System.out.println("Found the batch xml zip file.");
				outputStream = getOutputStreamFromZip(batchXMLPath, batchInstanceID + EXT_BATCH_XML_FILE);
				out.output(document, outputStream);
			} else {
				writer = new java.io.FileWriter(batchXMLPath);
				out.output(document, writer);
				writer.flush();
				writer.close();
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
		} finally {
			if (outputStream != null) {
				try {
					outputStream.close();
				} catch (IOException e) {
				}
			}
		}
	}

	public static OutputStream getOutputStreamFromZip(final String zipName, final String fileName) throws FileNotFoundException,
			IOException {
		ZipOutputStream stream = null;
		stream = new ZipOutputStream(new FileOutputStream(new File(zipName + ZIP_FILE_EXT)));
		ZipEntry zipEntry = new ZipEntry(fileName);
		stream.putNextEntry(zipEntry);
		return stream;
	}
}

package org.gridkit.data.extractors;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.junit.Test;

public class GenerateMessages {

	@Test
	public void generateMessages() throws IOException, InterruptedException {
		File path = new File("src/test/resources/protobuf");
		for(String f: path.list()) {
			if (f.endsWith(".msg")) {
				String obj = f.substring(0, f.lastIndexOf("."));
				String type = f.substring(0, f.indexOf("-"));
				System.err.println(f);
				ProcessBuilder pb = new ProcessBuilder();
				pb.directory(path);
				pb.command("protoc", "--encode=test." + type, "TestMessages.proto");
				Process p = pb.start();
				FileInputStream msg = new FileInputStream(new File(path, f));
				FileOutputStream out = new FileOutputStream(new File(path, obj + ".bin"));
				copy(msg, p.getOutputStream());
				p.getOutputStream().close();
				copy(p.getInputStream(), out);
				copy(p.getErrorStream(), System.err);
				p.waitFor();
				out.close();
			}
		}
	}
		
	static void copy(InputStream in, OutputStream out) throws IOException {
		byte[] buf = new byte[1 << 12];
		while(true) {
			int n = in.read(buf);
			if(n >= 0) {
				out.write(buf, 0, n);
			}
			else {
				break;
			}
		}
	}	
}

package pt.isel.ngspipes.engine_executor.utils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpUtils {

    public static void post(String url, String body) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        OutputStreamWriter wr= new OutputStreamWriter(conn.getOutputStream());
        wr.write(body);
        wr.flush();
        wr.close();
        validateSuccesOfRequest(conn);
    }

    public static String get(String url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestMethod("GET");
        validateSuccesOfRequest(conn);
        return readStream(conn.getInputStream());
    }

    public static void delete(String url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("DELETE");
        validateSuccesOfRequest(conn);
    }

    private static void validateSuccesOfRequest(HttpURLConnection conn) throws IOException {
        int responseCode = conn.getResponseCode();
        if(responseCode >= 400 && responseCode < 600)
            throw new IOException("Error " + responseCode + " - " + readStream(conn.getErrorStream()) + " - " + readStream(conn.getInputStream()));
    }


    private static String readStream(InputStream inputStream) throws IOException{
        if (inputStream == null)
            return "";

        BufferedReader br = null;
        String line;
        StringBuilder sb = new StringBuilder();

        try{
            br = new BufferedReader(new InputStreamReader(inputStream));

            while ((line = br.readLine()) != null)
                sb.append(line);

        } finally {
            if(br!=null)
                br.close();
        }

        return sb.toString();
    }

}

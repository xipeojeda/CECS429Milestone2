package cecs429.documents;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import com.google.gson.*;
import com.google.gson.stream.JsonReader;

public class JsonFileDocument implements FileDocument {
    private int documentId;
    private Path documentPath;
    private String title;

    public JsonFileDocument(int id, Path docPath) {
        documentId = id;
        documentPath = docPath;
    }

    @Override
    public Path getFilePath() {
        return documentPath;
    }

    @Override
    public int getId() {
        return documentId;
    }

    @Override
    public Reader getContent() {
        try (JsonReader jr = new JsonReader(new InputStreamReader(new FileInputStream(documentPath.toString())))){
            Gson gson = new Gson();
            JsonObject jsonObject = gson.fromJson(jr, JsonObject.class);
            jr.close();
            return new StringReader(jsonObject.get("body").toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getTitle() {
    	try (JsonReader jr = new JsonReader(new InputStreamReader(new FileInputStream(documentPath.toString())))){
			Gson gson = new Gson();
			JsonObject js = gson.fromJson(jr, JsonObject.class);
			title = js.get("title").getAsString();
			setTitle(title);
			jr.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
		return title;
    }

    public static FileDocument loadJsonFileDocument(Path absolutePath, int documentId) {
        return new JsonFileDocument(documentId, absolutePath);
    }

	public void setTitle(String title) {
		this.title = title;
	}

	@Override
	public String getFileName() {
		// TODO Auto-generated method stub
		return documentPath.getFileName().toString();
	}
}

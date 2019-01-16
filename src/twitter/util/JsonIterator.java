package twitter.util;

import com.google.gson.stream.JsonReader;
import java.io.IOException;
import java.util.Iterator;

/**
 * A wrapper class for a {@link JsonReader} that parses its contents and retrieves its
 * 'body' field to be a Twitter message.
 */
public class JsonIterator implements Iterator<String> {
  private final JsonReader reader;

  JsonIterator(JsonReader reader) {
    if (reader == null) {
      throw new IllegalArgumentException("Given reader was null");
    }
    this.reader = reader;
  }

  @Override
  public boolean hasNext() {
    boolean result = false;
    try {
      while (this.reader.hasNext()) {
        String name = this.reader.nextName();
        if (name.equals("body")) {
          result = true;
          break;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return result;
  }

  @Override
  public String next() {
    if (!this.hasNext()) {
      throw new IllegalStateException("No more messages in this iterator");
    }
    return this.parseMessage();
  }

  /**
   * Helper method that retrieves the next 'body' field.
   *
   * @return the next 'body' field.
   */
  private String parseMessage() {
    String message = null;
    try {
      while (this.reader.hasNext()) {
        String name = this.reader.nextName();
        if (name.equals("body")) {
          String tempMessage = this.reader.nextString();
          if (tempMessage.length() < 140) {
            message = tempMessage;
            break;
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (message == null) {
      throw new IllegalStateException("No more messages in the iterator");
    }
    return message;
  }
}

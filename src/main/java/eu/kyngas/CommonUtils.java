package eu.kyngas;

public class CommonUtils {

  public static void check(boolean check, Runnable ifTrue, Runnable ifFalse) {
    if (check) {
      ifTrue.run();
    } else {
      ifFalse.run();
    }
  }

}
package io.cdap.cdap.common;

import ch.qos.logback.classic.pattern.ExtendedThrowableProxyConverter;
import ch.qos.logback.classic.spi.IThrowableProxy;

import java.util.ArrayList;
import java.util.List;

public class SingleLineExtendedThrowableConverter extends ExtendedThrowableProxyConverter {
  private String regex = "\n";
  private String replacement = "|";

  @Override
  public void start() {
    List<String> options = getOptionList();
    if (options != null && !options.isEmpty()) {

      options = new ArrayList<>(getOptionList());
      if (options.size() == 1) {
        replacement = options.remove(0);
      } else {
        regex = options.remove(0);
        replacement = options.remove(0);
      }
      super.setOptionList(options);
    }

    super.start();
  }

  @Override
  protected String throwableProxyToString(IThrowableProxy tp) {
    String original = super.throwableProxyToString(tp);

    return original.replaceAll(regex, replacement);
  }
}

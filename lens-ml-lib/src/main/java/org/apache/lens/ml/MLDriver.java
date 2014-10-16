package org.apache.lens.ml;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.LensException;

import java.util.List;

public interface MLDriver {
  public boolean isTrainerSupported(String trainer);
  public MLTrainer getTrainerInstance(String trainer) throws LensException;
  public void init(LensConf conf) throws LensException;
  public void start() throws LensException;
  public void stop() throws LensException;
  public List<String> getTrainerNames();
}

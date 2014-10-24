package org.apache.lens.cli.commands;

import com.google.common.base.Joiner;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * The Class LensNativeTableCommands.
 */
@Component
public class LensNativeTableCommands extends BaseLensCommand implements CommandMarker {

  /**
   * Show native tables.
   *
   * @return the string
   */
  @CliCommand(value = "show nativetables", help = "show list of native tables")
  public String showNativeTables() {
    List<String> nativetables = getClient().getAllNativeTables();
    if (nativetables != null) {
      return Joiner.on("\n").join(nativetables);
    } else {
      return "No native tables found";
    }
  }

  /**
   * Describe native table.
   *
   * @param tblName
   *          the tbl name
   * @return the string
   */
  @CliCommand(value = "describe nativetable", help = "describe nativetable")
  public String describeNativeTable(
      @CliOption(key = { "", "nativetable" }, mandatory = true, help = "<native-table-name>") String tblName) {

    try {
      return formatJson(mapper.writer(pp).writeValueAsString(getClient().getNativeTable(tblName)));
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }
}

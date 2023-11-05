import {
  ILanguageServerPlugin,
  IConnectionDriverConstructor,
} from "@sqltools/types";
import TrinoDriver from "./driver";
import { DRIVER_ALIASES } from "./../constants";

const TrinoDriverPlugin: ILanguageServerPlugin = {
  register(server) {
    DRIVER_ALIASES.forEach(({ value }) => {
      server
        .getContext()
        .drivers.set(value, TrinoDriver as IConnectionDriverConstructor);
    });
  },
};

export default TrinoDriverPlugin;

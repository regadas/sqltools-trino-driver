import {
  ILanguageServerPlugin,
  IConnectionDriverConstructor,
} from "@sqltools/types";
import TrinoDriver from "./driver";
import { DRIVER_ALIASES } from "./../constants";

const YourDriverPlugin: ILanguageServerPlugin = {
  register(server) {
    DRIVER_ALIASES.forEach(({ value }) => {
      server
        .getContext()
        .drivers.set(value, TrinoDriver as IConnectionDriverConstructor);
    });
  },
};

export default YourDriverPlugin;

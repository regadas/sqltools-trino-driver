export class QueryParser {
  static statements(query: string, delimiter = ";"): string[] {
    return query
      .split(delimiter)
      .map((line) => line.trim())
      .filter((line) => line.length > 0);
  }
}

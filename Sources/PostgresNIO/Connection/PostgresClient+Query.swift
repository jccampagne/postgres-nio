import NIO
import Logging

public protocol PostgresBind {
    func postgresData(type: PostgresDataType) -> ByteBuffer?
}

extension PostgresDatabase {
    public func query(_ string: String, _ binds: [PostgresBind] = []) -> EventLoopFuture<[PostgresRow]> {
        var rows: [PostgresRow] = []
        return query(string, binds) { rows.append($0) }.map { rows }
    }
    
    public func query(_ string: String, _ binds: [PostgresBind] = [], _ onRow: @escaping (PostgresRow) throws -> ()) -> EventLoopFuture<Void> {
        let query = PostgresParameterizedQuery(query: string, binds: binds, onRow: onRow)
        return self.send(query, logger: self.logger)
    }
}

// MARK: Private

private final class PostgresParameterizedQuery: PostgresRequest {
    let query: String
    let binds: [PostgresBind]
    var onRow: (PostgresRow) throws -> ()
    var rowLookupTable: PostgresRow.LookupTable?
    var resultFormatCodes: [PostgresFormatCode]
    var logger: Logger?
    
    init(
        query: String,
        binds: [PostgresBind],
        onRow: @escaping (PostgresRow) throws -> ()
    ) {
        self.query = query
        self.binds = binds
        self.onRow = onRow
        self.resultFormatCodes = [.binary]
    }
    
    func log(to logger: Logger) {
        self.logger = logger
        logger.debug("\(self.query) \(self.binds)")
    }
    
    func respond(to message: PostgresMessage) throws -> [PostgresMessage]? {
        if case .error = message.identifier {
            // we should continue after errors
            return []
        }
        print(message)
        switch message.identifier {
        case .bindComplete:
            return []
        case .dataRow:
            let data = try PostgresMessage.DataRow(message: message)
            guard let rowLookupTable = self.rowLookupTable else { fatalError() }
            let row = PostgresRow(dataRow: data, lookupTable: rowLookupTable)
            try onRow(row)
            return []
        case .rowDescription:
            let row = try PostgresMessage.RowDescription(message: message)
            print(row)
            self.rowLookupTable = PostgresRow.LookupTable(
                rowDescription: row,
                resultFormat: self.resultFormatCodes
            )
            return []
        case .noData:
            return []
        case .parseComplete:
            return []
        case .parameterDescription:
            let params = try PostgresMessage.ParameterDescription(message: message)
            if params.dataTypes.count != self.binds.count {
                self.logger!.warning("Expected parameters count (\(params.dataTypes.count)) does not equal binds count (\(binds.count))")
            }
            let bind = PostgresMessage.Bind(
                portalName: "",
                statementName: "",
                parameterFormatCodes: self.binds.map { _ in .binary },
                parameters: zip(self.binds, params.dataTypes).map {
                    .init(value: $0.postgresData(type: $1))
                },
                resultFormatCodes: self.resultFormatCodes
            )
            let execute = PostgresMessage.Execute(
                portalName: "",
                maxRows: 0
            )
            let sync = PostgresMessage.Sync()
            return try [bind.message(), execute.message()]
        case .commandComplete:
            return []
        case .notice:
            return []
        case .readyForQuery:
            return nil
        default:
            throw PostgresError.protocol("Unexpected message during query: \(message)")
        }
    }
    
    func start() throws -> [PostgresMessage] {
        let parse = PostgresMessage.Parse(
            statementName: "",
            query: self.query,
            parameterTypes: []
        )
        let describe = PostgresMessage.Describe(
            command: .statement,
            name: ""
        )
        let sync = PostgresMessage.Sync()
        return try [parse.message(), describe.message(), sync.message()]
    }
}

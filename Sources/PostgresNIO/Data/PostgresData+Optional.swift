extension Optional: PostgresDataConvertible where Wrapped: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType {
        return Wrapped.postgresDataType
    }

    public init?(postgresData: PostgresData) {
        self = Wrapped.init(postgresData: postgresData)
    }
}

extension Optional: PostgresBind where Wrapped: PostgresBind {
    public func postgresData(type: PostgresDataType) -> ByteBuffer? {
        switch self {
        case .none:
            return nil
        case .some(let bind):
            return bind.postgresData(type: type)
        }
    }
}

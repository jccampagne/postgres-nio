extension PostgresData {
    public var bool: Bool? {
        guard var value = self.value else {
            return nil
        }
        guard value.readableBytes == 1 else {
            return nil
        }
        guard let byte = value.readInteger(as: UInt8.self) else {
            return nil
        }
        
        switch self.formatCode {
        case .text:
            switch byte {
            case Character("t").asciiValue!:
                return true
            case Character("f").asciiValue!:
                return false
            default:
                return nil
            }
        case .binary:
            switch byte {
            case 1:
                return true
            case 0:
                return false
            default:
                return nil
            }
        }
    }
}

extension Bool: PostgresDataConvertible {
    public static var postgresDataType: PostgresDataType {
        return .bool
    }
    
    public init?(postgresData: PostgresData) {
        guard let bool = postgresData.bool else {
            return nil
        }
        self = bool
    }
}

extension Bool: PostgresBind {
    public func postgresData(type: PostgresDataType) -> ByteBuffer? {
        switch type {
        case .bool:
            var buffer = ByteBufferAllocator().buffer(capacity: 1)
            buffer.writeInteger(self ? 1 : 0, as: UInt8.self)
            return buffer
        default:
            return nil
        }
    }
}

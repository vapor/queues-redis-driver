//
//  JobsRedisDriverError.swift
//  
//
//  Created by Jimmy McDermott on 6/11/19.
//

import Foundation

/// Describes an error from the JobsRedisDriver
public enum JobsRedisDriverError: Error {
    
    /// Could not convert data to RESPValue
    case couldNotConvertData
}

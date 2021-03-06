/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.rm.tcc.exception;

import io.seata.common.exception.FrameworkErrorCode;
import io.seata.common.exception.FrameworkException;

/**
 * TCC Fence Exception
 *
 * @author kaka2code
 */
public class TCCFenceException extends FrameworkException {

    public TCCFenceException(FrameworkErrorCode err) {
        super(err);
    }

    public TCCFenceException(String msg) {
        super(msg);
    }

    public TCCFenceException(String msg, FrameworkErrorCode errCode) {
        super(msg, errCode);
    }

    public TCCFenceException(Throwable cause, String msg, FrameworkErrorCode errCode) {
        super(cause, msg, errCode);
    }

    public TCCFenceException(Throwable th) {
        super(th);
    }

    public TCCFenceException(Throwable th, String msg) {
        super(th, msg);
    }

}

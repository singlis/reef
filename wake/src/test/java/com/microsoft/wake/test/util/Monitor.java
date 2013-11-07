/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.wake.test.util;

import java.util.concurrent.atomic.AtomicBoolean;

public class Monitor {
  private AtomicBoolean finished = new AtomicBoolean(false);
    
  public void mwait() throws InterruptedException {
    synchronized(this) {
      while(!finished.get()) { 
        this.wait();
      }
      finished.compareAndSet(true, false);
    }
  }
    
  public void mnotify() {
    synchronized(this) {
      finished.compareAndSet(false, true);
      this.notifyAll();
    }
  }
}

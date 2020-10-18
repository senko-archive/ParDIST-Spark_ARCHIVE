package com.senko.ParDISTonSpark

import java.util.concurrent.ThreadFactory

class CustomThreadFactory extends ThreadFactory {
  override def newThread(r: Runnable): Thread = {
    val t = new Thread(r)
    t.setDaemon(true)
    t
  }
}

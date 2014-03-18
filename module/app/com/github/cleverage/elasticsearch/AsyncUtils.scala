package com.github.cleverage.elasticsearch

import concurrent.{Future, Promise}
import org.elasticsearch.action.{ActionResponse, ActionRequest, ActionListener, ActionRequestBuilder}
import play.libs.F
import io.searchbox.client.{JestResultHandler, JestResult, JestClient}
import io.searchbox.Action
import com.github.cleverage.elasticsearch.jest.JestRichResult

/**
 * Utils for managing Asynchronous tasks
 */
object AsyncUtils {
  /**
   * Create a default promise
   * @return
   */
  def createPromise[T](): Promise[T] = Promise[T]()

  /**
   * Allows to execute asynchronously a request to the elasticsearch server
   * @param client
   * @param clientRequest
   * @return
   */
  def executeAsync(client: JestClient, clientRequest: Action): Future[JestRichResult] = {
    val promise = Promise[JestRichResult]()

    client.executeAsync(clientRequest, new JestResultHandler[JestResult] {
      override def failed(ex: Exception): Unit = promise.failure(ex)

      override def completed(result: JestResult): Unit = promise.success(new JestRichResult(result))
    })
    
    promise.future
  }
}

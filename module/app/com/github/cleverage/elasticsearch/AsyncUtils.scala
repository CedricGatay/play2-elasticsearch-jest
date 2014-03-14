package com.github.cleverage.elasticsearch

import concurrent.{Future, Promise}
import org.elasticsearch.action.{ActionResponse, ActionRequest, ActionListener, ActionRequestBuilder}
import play.libs.F
import io.searchbox.client.{JestResultHandler, JestResult, JestClient}
import io.searchbox.Action

/**
 * Utils for managing Asynchronous tasks
 */
object AsyncUtils {
  /**
   * Create a default promise
   * @return
   */
  def createPromise[T](): Promise[T] = Promise[T]()

  def executeAsync(client: JestClient, clientRequest: Action): Future[JestResult] = {
    val promise = Promise[JestResult]()

    client.executeAsync(clientRequest, new JestResultHandler[JestResult] {
      override def failed(ex: Exception): Unit = promise.failure(ex)

      override def completed(result: JestResult): Unit = promise.success(result)
    })
    
    promise.future
  }

  /**
   * Execute an Elasticsearch request asynchronously
   * @param requestBuilder
   * @return
   */
  def executeAsync[RQ <: ActionRequest[RQ], RS <: ActionResponse, RB <: ActionRequestBuilder[RQ, RS, RB]](requestBuilder: ActionRequestBuilder[RQ, RS, RB]): Future[RS] = {
    val promise = Promise[RS]()

    requestBuilder.execute(new ActionListener[RS] {
      def onResponse(response: RS) {
        promise.success(response)
      }

      def onFailure(t: Throwable) {
        promise.failure(t)
      }
    })

    promise.future
  }

  /**
   * Execute an Elasticsearch request asynchronously and return a Java Promise
   * @param requestBuilder
   * @return
   */
  def executeAsyncJava[RQ <: ActionRequest[RQ], RS <: ActionResponse, RB <: ActionRequestBuilder[RQ, RS, RB]](requestBuilder: ActionRequestBuilder[RQ, RS, RB]): F.Promise[RS] = {
    F.Promise.wrap(executeAsync(requestBuilder))
  }

}

import { HttpRequest, HttpHandlerFn, HttpEvent } from "@angular/common/http";
import { Observable } from "rxjs";

export function authInterceptor(
  req: HttpRequest<unknown>,
  next: HttpHandlerFn,
): Observable<HttpEvent<unknown>> {
  let reqClone = req.clone({ setHeaders: { Authorization: 'Bearer driva_test_key_abc123xyz789' } });
  return next(reqClone);
}

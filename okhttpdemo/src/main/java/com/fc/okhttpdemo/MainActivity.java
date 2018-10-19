package com.fc.okhttpdemo;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.widget.Button;
import android.widget.TextView;
import android.widget.Toast;

import java.io.IOException;

import butterknife.BindView;
import butterknife.ButterKnife;
import butterknife.OnClick;
import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.ObservableOnSubscribe;
import io.reactivex.android.schedulers.AndroidSchedulers;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.observers.DisposableObserver;
import io.reactivex.schedulers.Schedulers;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class MainActivity extends AppCompatActivity {

    public final String URL = "http://www.weather.com.cn/adat/sk/101010100.html";

    CompositeDisposable compositeDisposable = new CompositeDisposable();

    @BindView(R.id.tv_info)
    TextView tvInfo;
    @BindView(R.id.btn_sync)
    Button btn_sync;
    @BindView(R.id.btn_async)
    Button btnAsync;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        ButterKnife.bind(this);


    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        compositeDisposable.clear();
    }

    @OnClick({R.id.btn_sync})
    public void onViewClicked() {


        Observable<String> stringObservable = Observable.create(new ObservableOnSubscribe<String>() {
            @Override
            public void subscribe(final ObservableEmitter<String> eo) throws Exception {

//                syncRequest(eo);

                asyncRequest(eo);
            }
        });

        DisposableObserver<String> disposableObserver = new DisposableObserver<String>() {
            @Override
            public void onNext(String s) {

                tvInfo.setText(s);
            }

            @Override
            public void onError(Throwable e) {

                tvInfo.setText(e.getMessage());
            }

            @Override
            public void onComplete() {

                Toast.makeText(MainActivity.this, "请求成功", Toast.LENGTH_SHORT).show();
            }
        };
        stringObservable.observeOn(AndroidSchedulers.mainThread())
                .subscribeOn(Schedulers.io())
                .subscribe(disposableObserver);

        compositeDisposable.add(disposableObserver);

    }


    public void asyncRequest(final ObservableEmitter<String> eo) {
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder().url(URL).build();
        Call call = client.newCall(request);
        String back;
        try {
            Response execute = call.execute();
            back = execute.body().string();
            eo.onNext(back);
            eo.onComplete();
        } catch (IOException e) {
            e.printStackTrace();
            eo.onError(e);
        }


    }


    /**
     * 同步请求
     *
     * @param eo
     * @throws IOException
     */
    private void syncRequest(final ObservableEmitter<String> eo) throws IOException {
        OkHttpClient okHttpClient = new OkHttpClient();
        Request request = new Request.Builder().url(URL).build();
        Call call = okHttpClient.newCall(request);
        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, final IOException e) {

                eo.onError(e);

            }

            @Override
            public void onResponse(Call call, final Response response) throws IOException {

                eo.onNext(response.body().string());

                eo.onComplete();
            }
        });
    }

    @OnClick(R.id.btn_async)
    public void onViewClicked2() {


    }
}

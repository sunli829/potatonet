extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::quote;
use syn::spanned::Spanned;
use syn::{
    parse_macro_input, AngleBracketedGenericArguments, Attribute, Block, DeriveInput, Error, FnArg,
    GenericArgument, ImplItem, ImplItemMethod, ItemImpl, LitStr, Meta, NestedMeta, Pat, PatIdent,
    PathArguments, Result, ReturnType, Type, TypePath,
};

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum MethodType {
    Call,
    Notify,
}

struct MethodInfo {
    ty: MethodType,
    name: Option<LitStr>,
}

enum MethodResult<'a> {
    Default,
    Value(&'a TypePath),
    Result(&'a TypePath),
}

struct Method<'a> {
    ty: MethodType,
    name: Ident,
    context: Option<&'a PatIdent>,
    args: Vec<(&'a PatIdent, &'a TypePath)>,
    result: MethodResult<'a>,
    block: &'a Block,
}

/// 解析method定义
fn parse_method_info(attrs: &[Attribute]) -> Option<MethodInfo> {
    for attr in attrs {
        match attr.parse_meta() {
            Ok(Meta::Path(path)) => {
                if path.is_ident("call") {
                    return Some(MethodInfo {
                        ty: MethodType::Call,
                        name: None,
                    });
                } else if path.is_ident("notify") {
                    return Some(MethodInfo {
                        ty: MethodType::Notify,
                        name: None,
                    });
                }
            }
            Ok(Meta::List(list)) => {
                let ty = if list.path.is_ident("call") {
                    Some(MethodType::Call)
                } else if list.path.is_ident("notify") {
                    Some(MethodType::Notify)
                } else {
                    None
                };

                if let Some(ty) = ty {
                    let mut name = None;
                    for arg in list.nested {
                        if let NestedMeta::Meta(Meta::NameValue(nv)) = arg {
                            if nv.path.is_ident("name") {
                                if let syn::Lit::Str(lit) = nv.lit {
                                    name = Some(lit);
                                }
                            }
                        }
                    }
                    return Some(MethodInfo { ty, name });
                }
            }
            _ => {}
        }
    }

    None
}

/// 解析method
fn parse_method(info: MethodInfo, method: &ImplItemMethod) -> Result<Method> {
    let name = info
        .name
        .map(|lit| Ident::new(&lit.value(), lit.span()))
        .unwrap_or(method.sig.ident.clone());

    if method.sig.asyncness.is_none() {
        return Err(Error::new(method.span(), "invalid method"));
    }

    // 解析参数
    let mut args = Vec::new();
    let mut context = None;
    for (idx, arg) in method.sig.inputs.iter().enumerate() {
        if let FnArg::Receiver(receiver) = arg {
            if idx != 0 {
                // self必须是第一个参数
                return Err(Error::new(receiver.span(), "invalid method"));
            }
            if receiver.mutability.is_some() {
                // 不能是可变借用
                return Err(Error::new(receiver.mutability.span(), "invalid method"));
            }
        } else if let FnArg::Typed(pat) = arg {
            if idx == 0 {
                // 第一个参数必须是self
                return Err(Error::new(pat.span(), "invalid method"));
            }

            match (&*pat.pat, &*pat.ty) {
                // 参数
                (Pat::Ident(id), Type::Path(ty)) => args.push((id, ty)),
                // Context
                (Pat::Ident(id), Type::Reference(ty)) => {
                    if idx != 1 {
                        // context必须是第二个参数
                        return Err(Error::new(pat.span(), "invalid method"));
                    }

                    if ty.mutability.is_some() {
                        // context必须是不可变借用
                        return Err(Error::new(pat.span(), "invalid method"));
                    }

                    if let Type::Path(path) = ty.elem.as_ref() {
                        if path.path.segments.last().unwrap().ident.to_string() == "NodeContext" {
                            let seg = &path.path.segments.last().unwrap();
                            if let PathArguments::AngleBracketed(angle_args) = &seg.arguments {
                                if angle_args.args.len() != 1 {
                                    // context的泛型参数错误
                                    return Err(Error::new(pat.span(), "invalid method"));
                                }
                                if let GenericArgument::Lifetime(life) = &angle_args.args[0] {
                                    if life.ident.to_string() != "_" {
                                        // context的泛型参数错误
                                        return Err(Error::new(pat.span(), "invalid method"));
                                    }
                                    context = Some(id);
                                } else {
                                    // context的泛型参数错误
                                    return Err(Error::new(pat.span(), "invalid method"));
                                }
                            } else {
                                // context的泛型参数错误
                                return Err(Error::new(pat.span(), "invalid method"));
                            }
                        } else {
                            // 不是context类型
                            return Err(Error::new(pat.span(), "invalid method"));
                        }
                    } else {
                        // 不是context类型
                        return Err(Error::new(pat.span(), "invalid method"));
                    }
                }
                _ => return Err(Error::new(pat.span(), "invalid method")),
            }
        }
    }

    // 解析返回值
    let result = match info.ty {
        MethodType::Call => {
            match &method.sig.output {
                ReturnType::Default => MethodResult::Default,
                ReturnType::Type(_, ty) => {
                    if let Type::Path(type_path) = ty.as_ref() {
                        let is_result = if type_path.path.segments.len() == 1 {
                            type_path.path.segments[0].ident.to_string() == "Result"
                        } else {
                            false
                        };

                        if is_result {
                            if let PathArguments::AngleBracketed(AngleBracketedGenericArguments {
                                args,
                                ..
                            }) = &type_path.path.segments[0].arguments
                            {
                                if args.len() != 1 {
                                    // 错误的result类型
                                    return Err(Error::new(
                                        method.sig.output.span(),
                                        "invalid method",
                                    ));
                                }
                                let value = match &args[0] {
                                    GenericArgument::Type(Type::Path(path)) => path,
                                    _ => {
                                        return Err(Error::new(
                                            method.sig.output.span(),
                                            "invalid method",
                                        ))
                                    }
                                };
                                MethodResult::Result(value)
                            } else {
                                // 错误的result类型
                                return Err(Error::new(method.sig.output.span(), "invalid method"));
                            }
                        } else {
                            MethodResult::Value(type_path)
                        }
                    } else {
                        // 不支持的返回值类型
                        return Err(Error::new(method.sig.output.span(), "invalid method"));
                    }
                }
            }
        }
        MethodType::Notify => {
            // notify不能有返回值
            match method.sig.output {
                ReturnType::Default => MethodResult::Default,
                _ => return Err(Error::new(method.sig.output.span(), "invalid method")),
            }
        }
    };

    Ok(Method {
        ty: info.ty,
        name,
        context,
        args,
        result,
        block: &method.block,
    })
}

#[proc_macro_attribute]
pub fn service(_args: TokenStream, input: TokenStream) -> TokenStream {
    let impl_item = parse_macro_input!(input as ItemImpl);
    let (self_ty, self_name) = match impl_item.self_ty.as_ref() {
        Type::Path(path) => (
            path,
            path.path
                .segments
                .last()
                .map(|s| s.ident.to_string())
                .unwrap(),
        ),
        _ => {
            return Error::new(impl_item.span(), "invalid method")
                .to_compile_error()
                .into()
        }
    };
    let client_ty = Ident::new(&format!("{}Client", self_name), self_ty.span());
    let req_type_name = Ident::new(&format!("__RequestType_{}", self_name), self_ty.span());
    let rep_type_name = Ident::new(&format!("__ResponseType{}", self_name), self_ty.span());
    let notify_type_name = Ident::new(&format!("__NotifyType{}", self_name), self_ty.span());
    let mut methods = Vec::new();
    let mut other_methods = Vec::new();
    let mut internal_methods = Vec::new();

    for item in &impl_item.items {
        if let ImplItem::Method(method) = item {
            let ident = method.sig.ident.to_string();
            if let Some(method_info) = parse_method_info(&method.attrs) {
                let method = match parse_method(method_info, method) {
                    Ok(method) => method,
                    Err(err) => return err.to_compile_error().into(),
                };
                methods.push(method);
            } else if ident == "start" || ident == "stop" {
                // 开始或者停止服务
                other_methods.push(item);
            } else {
                // 内部函数
                internal_methods.push(item);
            }
        }
    }

    let expanded = {
        // 请求类型
        let req_type = {
            let mut reqs = Vec::new();
            for method in methods
                .iter()
                .filter(|method| method.ty == MethodType::Call)
            {
                let name = Ident::new(&method.name.to_string().to_uppercase(), method.name.span());
                let types = method.args.iter().map(|(_, ty)| ty).collect::<Vec<_>>();
                reqs.push(quote! { #name(#(#types),*) });
            }
            quote! {
                #[derive(potatonet_node::serde_derive::Serialize, potatonet_node::serde_derive::Deserialize)]
                pub enum #req_type_name { #(#reqs),* }
            }
        };

        // 响应类型
        let rep_type = {
            let mut reps = Vec::new();
            for method in methods
                .iter()
                .filter(|method| method.ty == MethodType::Call)
            {
                let name = Ident::new(&method.name.to_string().to_uppercase(), method.name.span());
                match &method.result {
                    MethodResult::Value(ty) => reps.push(quote! { #name(#ty) }),
                    MethodResult::Result(ty) => reps.push(quote! { #name(#ty) }),
                    MethodResult::Default => {}
                }
            }
            quote! {
                #[derive(potatonet_node::serde_derive::Serialize, potatonet_node::serde_derive::Deserialize)]
                pub enum #rep_type_name { #(#reps),* }
            }
        };

        // 通知类型
        let notify_type = {
            let mut notify = Vec::new();
            for method in methods
                .iter()
                .filter(|method| method.ty == MethodType::Notify)
            {
                let name = Ident::new(&method.name.to_string().to_uppercase(), method.name.span());
                let types = method.args.iter().map(|(_, ty)| ty).collect::<Vec<_>>();
                notify.push(quote! { #name(#(#types),*) });
            }
            quote! {
                #[derive(potatonet_node::serde_derive::Serialize, potatonet_node::serde_derive::Deserialize)]
                pub enum #notify_type_name { #(#notify),* }
            }
        };

        // 请求处理代码
        let req_handler = {
            let mut list = Vec::new();

            for method in methods
                .iter()
                .filter(|method| method.ty == MethodType::Call)
            {
                let method_name = method.name.to_string();
                let vars = method.args.iter().map(|(name, _)| name).collect::<Vec<_>>();
                let name = Ident::new(&method.name.to_string().to_uppercase(), method.name.span());
                let block = method.block;
                let ctx = match method.context {
                    Some(id) => quote! { let #id = ctx; },
                    None => quote! {},
                };

                match &method.result {
                    MethodResult::Default => {
                        list.push(quote! {
                            if request.method == #method_name {
                                if let #req_type_name::#name(#(#vars),*) = request.data {
                                    #ctx
                                    return Ok(potatonet_node::Response::new(#rep_type_name::#name(#block)));
                                }
                            }
                        });
                    }
                    MethodResult::Value(_) => {
                        list.push(quote! {
                            if request.method == #method_name {
                                if let #req_type_name::#name(#(#vars),*) = request.data {
                                    #ctx
                                    let res = #block;
                                    return Ok(potatonet_node::Response::new(#rep_type_name::#name(res)));
                                }
                            }
                        });
                    }
                    MethodResult::Result(_) => {
                        list.push(quote! {
                            if request.method == #method_name {
                                if let #req_type_name::#name(#(#vars),*) = request.data {
                                    #ctx
                                    let res: potatonet_node::Result<potatonet_node::Response<Self::Rep>> = #block.map(|x| potatonet_node::Response::new(#rep_type_name::#name(x)));
                                    return res;
                                }
                            }
                        });
                    }
                }
            }

            quote! { #(#list)* }
        };

        // 通知处理代码
        let notify_handler = {
            let mut list = Vec::new();

            for method in methods
                .iter()
                .filter(|method| method.ty == MethodType::Notify)
            {
                let method_name = method.name.to_string();
                let vars = method.args.iter().map(|(name, _)| name).collect::<Vec<_>>();
                let name = Ident::new(&method.name.to_string().to_uppercase(), method.name.span());
                let ctx = match method.context {
                    Some(id) => quote! { let #id = ctx; },
                    None => quote! {},
                };
                let block = method.block;

                list.push(quote! {
                    if request.method == #method_name {
                        if let #notify_type_name::#name(#(#vars),*) = request.data {
                            #ctx
                            #block
                        }
                    }
                });
            }

            quote! { #(#list)* }
        };

        // 客户端函数
        let client_methods = {
            let mut client_methods = Vec::new();
            for method in &methods {
                let client_method = {
                    let method_name = &method.name;
                    let name =
                        Ident::new(&method.name.to_string().to_uppercase(), method.name.span());
                    let method_str = method_name.to_string();
                    let params = method.args.iter().map(|(name, ty)| {
                        quote! { #name: #ty }
                    });
                    let vars = method.args.iter().map(|(name, _)| name).collect::<Vec<_>>();
                    match method.ty {
                        MethodType::Call => {
                            let res_type = match &method.result {
                                MethodResult::Default => quote! { () },
                                MethodResult::Value(value) => quote! { #value },
                                MethodResult::Result(value) => quote! { #value },
                            };
                            quote! {
                                pub async fn #method_name(&self, #(#params),*) -> potatonet_node::Result<#res_type> {
                                    let res = self.ctx.call::<_, #rep_type_name>(&self.service_name, potatonet_node::Request::new(#method_str, #req_type_name::#name(#(#vars),*))).await?;
                                    if let potatonet_node::Response{data: #rep_type_name::#name(value)} = res {
                                        Ok(value)
                                    } else {
                                        unreachable!()
                                    }
                                }
                            }
                        }
                        MethodType::Notify => {
                            quote! {
                                pub async fn #method_name(&self, #(#params),*) {
                                    self.ctx.notify(&self.service_name, potatonet_node::Request::new(#method_str, #notify_type_name::#name(#(#vars),*))).await
                                }
                            }
                        }
                    }
                };
                client_methods.push(client_method);
            }
            client_methods
        };

        quote! {
            #[allow(non_camel_case_types)] #req_type
            #[allow(non_camel_case_types)] #rep_type
            #[allow(non_camel_case_types)] #notify_type

            // 服务代码
            #[potatonet_node::async_trait::async_trait]
            impl potatonet_node::Service for #self_ty {
                type Req = #req_type_name;
                type Rep = #rep_type_name;
                type Notify = #notify_type_name;

                #(#other_methods)*

                #[allow(unused_variables)]
                async fn call(&self, ctx: &potatonet_node::NodeContext<'_>, request: potatonet_node::Request<Self::Req>) ->
                    potatonet_node::Result<potatonet_node::Response<Self::Rep>> {
                    #req_handler
                    Err(potatonet_node::Error::MethodNotFound { method: request.method.clone() }.into())
                }

                #[allow(unused_variables)]
                async fn notify(&self, ctx: &potatonet_node::NodeContext<'_>, request: potatonet_node::Request<Self::Notify>) {
                    #notify_handler
                }
            }

            impl potatonet_node::NamedService for #self_ty {
                fn name(&self) -> &'static str {
                    #self_name
                }
            }

            impl #self_ty {
                #(#internal_methods)*
            }

            // 客户端代码
            pub struct #client_ty<'a, C> {
                ctx: &'a C,
                service_name: std::borrow::Cow<'a, str>,
            }

            impl<'a, C: potatonet_node::Context> #client_ty<'a, C> {
                pub fn new(ctx: &'a C) -> Self {
                    Self { ctx, service_name: std::borrow::Cow::Borrowed(#self_name) }
                }

                pub fn with_name<N>(ctx: &'a C, name: N) -> Self
                where N: Into<std::borrow::Cow<'a, str>> {
                    Self { ctx, service_name: name.into() }
                }

                #(#client_methods)*
            }
        }
    };

    expanded.into()
}

#[proc_macro_attribute]
pub fn message(_args: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let expanded = quote! {
        #[derive(potatonet_node::serde_derive::Serialize, potatonet_node::serde_derive::Deserialize)]
        #input
    };
    expanded.into()
}
